use std::collections::BTreeMap;
use std::fmt::Display;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use bytes::Bytes;
use clap::Parser;
use http::Request;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use http;

const RAMP_STEP: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Serialize)]
pub enum Protocol {
    Http1,
    Http2,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Stage {
    pub duration: Duration,
    pub target: f64,
}

#[derive(Clone, Debug, Serialize)]
pub enum Load {
    ConstantVus { duration: Duration },
    ConstantArrivalRate { rate: f64, duration: Duration },
    RampingArrivalRate { start_rate: f64, stages: Vec<Stage> },
}

impl Load {
    fn steps(&self) -> Vec<(Duration, Option<f64>)> {
        match self {
            Load::ConstantVus { duration } => vec![(*duration, None)],
            Load::ConstantArrivalRate { rate, duration } => vec![(*duration, Some(*rate))],
            Load::RampingArrivalRate { start_rate, stages } => {
                // ponytail: linear ramps run as RAMP_STEP-long oha runs, each one reconnects.
                // Upgrade path: drive oha's client directly with a rate schedule.
                let mut steps = Vec::new();
                let mut from = *start_rate;
                for &Stage { duration, target } in stages {
                    let n = (duration.as_secs_f64() / RAMP_STEP.as_secs_f64()).ceil() as u32;
                    for i in 0..n {
                        let t = (i as f64 + 0.5) / n as f64;
                        steps.push((duration / n, Some(from + (target - from) * t)));
                    }
                    from = target;
                }
                steps
            }
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct Config {
    target: String,
    protocol: Protocol,
    connections: usize,
    streams: usize,
    warm_up_time: Duration,
    load: Load,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Trend {
    pub avg: f64,
    pub min: f64,
    pub med: f64,
    pub max: f64,
    #[serde(rename = "p(90)")]
    pub p90: f64,
    #[serde(rename = "p(95)")]
    pub p95: f64,
    #[serde(rename = "p(99)")]
    pub p99: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Counter {
    pub count: u64,
    pub rate: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Rate {
    pub passes: u64,
    pub fails: u64,
    pub value: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Metrics {
    pub http_req_duration: Trend,
    pub http_reqs: Counter,
    pub data_received: Counter,
    pub http_req_failed: Rate,
    pub vus_max: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Measurement {
    pub metrics: Metrics,
    pub status_codes: BTreeMap<u16, u64>,
    pub errors: BTreeMap<String, u64>,
}

#[derive(Deserialize)]
struct OhaReport {
    summary: OhaSummary,
    #[serde(rename = "errorDistribution")]
    error_distribution: BTreeMap<String, u64>,
}

#[derive(Deserialize)]
struct OhaSummary {
    total: f64,
}

pub struct Bencher {
    request: Option<Request<Bytes>>,
}

impl Bencher {
    pub fn iter<B: Into<Bytes>, F: FnMut() -> Request<B>>(&mut self, mut routine: F) {
        // ponytail: oha replays one fixed request, so the routine runs once.
        // Upgrade path: drive oha's client with a request per call.
        self.request = Some(routine().map(Into::into));
    }
}

#[derive(Clone, Debug, Default)]
pub struct BenchmarkId {
    function: Option<String>,
    parameter: Option<String>,
}

impl BenchmarkId {
    pub fn new(function: impl Into<String>, parameter: impl Display) -> Self {
        BenchmarkId {
            function: Some(function.into()),
            parameter: Some(parameter.to_string()),
        }
    }

    pub fn from_parameter(parameter: impl Display) -> Self {
        BenchmarkId {
            function: None,
            parameter: Some(parameter.to_string()),
        }
    }
}

impl From<&str> for BenchmarkId {
    fn from(function: &str) -> Self {
        BenchmarkId {
            function: Some(function.to_string()),
            parameter: None,
        }
    }
}

impl From<String> for BenchmarkId {
    fn from(function: String) -> Self {
        BenchmarkId {
            function: Some(function),
            parameter: None,
        }
    }
}

pub struct Criterion {
    config: Config,
    filter: Option<String>,
    output_directory: PathBuf,
    runtime: tokio::runtime::Runtime,
}

impl Default for Criterion {
    fn default() -> Self {
        Criterion {
            config: Config {
                target: String::new(),
                protocol: Protocol::Http1,
                connections: 50,
                streams: 1,
                warm_up_time: Duration::from_secs(3),
                load: Load::ConstantVus {
                    duration: Duration::from_secs(10),
                },
            },
            filter: None,
            output_directory: cargo_target_directory().join("gumo"),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("gumo: cannot build the tokio runtime"),
        }
    }
}

impl Criterion {
    pub fn configure_from_args(mut self) -> Self {
        self.filter = std::env::args().skip(1).find(|arg| !arg.starts_with('-'));
        self
    }

    pub fn target(mut self, target: impl Into<String>) -> Self {
        self.config.target = target.into();
        self
    }

    pub fn protocol(mut self, protocol: Protocol) -> Self {
        self.config.protocol = protocol;
        self
    }

    pub fn connections(mut self, connections: usize) -> Self {
        self.config.connections = connections;
        self
    }

    pub fn streams(mut self, streams: usize) -> Self {
        self.config.streams = streams;
        self
    }

    pub fn warm_up_time(mut self, warm_up_time: Duration) -> Self {
        self.config.warm_up_time = warm_up_time;
        self
    }

    pub fn load(mut self, load: Load) -> Self {
        self.config.load = load;
        self
    }

    pub fn benchmark_group(&mut self, name: impl Into<String>) -> BenchmarkGroup<'_> {
        BenchmarkGroup {
            config: self.config.clone(),
            criterion: self,
            name: name.into(),
        }
    }

    pub fn bench_function<F: FnMut(&mut Bencher)>(&mut self, id: &str, f: F) -> &mut Self {
        self.benchmark_group(id)
            .bench_function(BenchmarkId::default(), f);
        self
    }

    fn run<F: FnMut(&mut Bencher)>(&self, config: &Config, ids: Vec<String>, mut f: F) {
        let full_id = ids.join("/");
        if let Some(filter) = &self.filter
            && !full_id.contains(filter.as_str())
        {
            return;
        }
        assert!(
            !config.target.is_empty(),
            "gumo: {full_id} has no target address"
        );

        let mut bencher = Bencher { request: None };
        f(&mut bencher);
        let Some(request) = bencher.request else {
            panic!("gumo: {full_id} did not call Bencher::iter");
        };

        let dir = ids.iter().fold(self.output_directory.clone(), |dir, id| {
            dir.join(make_filename_safe(id))
        });
        let new = dir.join("new");
        let base = dir.join("base");
        if new.exists() {
            if base.exists() {
                fs::remove_dir_all(&base).expect("gumo: cannot remove the old base");
            }
            fs::rename(&new, &base).expect("gumo: cannot move new to base");
        }
        fs::create_dir_all(&new).expect("gumo: cannot create the output directory");
        write_json(
            &new.join("benchmark.json"),
            &serde_json::json!({ "full_id": full_id, "config": config }),
        );

        let (parts, body) = request.into_parts();
        let http::request::Parts {
            method,
            uri,
            headers,
            ..
        } = parts;
        let path = uri.path_and_query().map_or("/", |path| path.as_str());
        let mut args = vec![
            "oha".to_string(),
            format!("{}{path}", config.target.trim_end_matches('/')),
            "--no-tui".into(),
            "--output-format".into(),
            "json".into(),
            "-w".into(),
            "-m".into(),
            method.to_string(),
            "-c".into(),
            config.connections.to_string(),
            "-p".into(),
            config.streams.to_string(),
        ];
        let vus_max = match config.protocol {
            Protocol::Http1 => config.connections,
            Protocol::Http2 => {
                args.push("--http2".into());
                config.connections * config.streams
            }
        };
        for (name, value) in &headers {
            let value = value
                .to_str()
                .unwrap_or_else(|_| panic!("gumo: header {name} is not visible ASCII"));
            args.extend(["-H".into(), format!("{name}: {value}")]);
        }
        if !body.is_empty() {
            let body_path = new.join("body.bin");
            fs::write(&body_path, &body).expect("gumo: cannot write the request body");
            args.extend(["-D".into(), path_arg(&body_path)]);
        }

        if !config.warm_up_time.is_zero() {
            let mut args = args.clone();
            args.extend(["-z".into(), duration_arg(config.warm_up_time)]);
            self.oha(args, &new.join("warmup.json"));
        }

        let db = new.join("requests.db");
        let mut total = 0.0;
        let mut errors = BTreeMap::new();
        for (i, (duration, rate)) in config.load.steps().into_iter().enumerate() {
            let mut args = args.clone();
            args.extend([
                "-z".into(),
                duration_arg(duration),
                "--db-url".into(),
                path_arg(&db),
            ]);
            match rate {
                // oha reads a rate of 0 as "no limit", so a zero-rate step sends nothing.
                Some(rate) if rate <= 0.0 => {
                    total += duration.as_secs_f64();
                    continue;
                }
                Some(rate) => args.extend(["-q".into(), rate.to_string()]),
                None => {}
            }
            let OhaReport {
                summary: OhaSummary { total: step_total },
                error_distribution,
            } = self.oha(args, &new.join(format!("oha-{i}.json")));
            total += step_total;
            for (error, count) in error_distribution {
                *errors.entry(error).or_default() += count;
            }
        }

        let measurement = measure(&db, total, vus_max, errors);
        let summary =
            serde_json::to_value(&measurement).expect("gumo: cannot serialize the summary");
        write_json(&new.join("summary.json"), &summary);

        let base = fs::read(base.join("summary.json"))
            .ok()
            .and_then(|base| serde_json::from_slice::<Value>(&base).ok());
        let change = base.and_then(|base| relative_change(&summary, &base));
        let change_dir = dir.join("change");
        // A base without summary.json (crashed run) must not leave an older change behind.
        let _ = fs::remove_dir_all(&change_dir);
        if let Some(change) = &change {
            fs::create_dir_all(&change_dir).expect("gumo: cannot create the change directory");
            write_json(&change_dir.join("summary.json"), change);
        }
        print_measurement(&full_id, &measurement, change.as_ref());
    }

    fn oha(&self, mut args: Vec<String>, output: &Path) -> OhaReport {
        args.extend(["-o".into(), path_arg(output)]);
        let opts = oha::Opts::try_parse_from(&args)
            .unwrap_or_else(|e| panic!("gumo: invalid oha arguments {args:?}: {e}"));
        self.runtime
            .block_on(oha::run(opts))
            .unwrap_or_else(|e| panic!("gumo: oha failed: {e}"));
        let report = fs::read(output).expect("gumo: cannot read the oha report");
        serde_json::from_slice(&report).expect("gumo: cannot parse the oha report")
    }
}

pub struct BenchmarkGroup<'a> {
    criterion: &'a Criterion,
    name: String,
    config: Config,
}

impl BenchmarkGroup<'_> {
    pub fn target(&mut self, target: impl Into<String>) -> &mut Self {
        self.config.target = target.into();
        self
    }

    pub fn protocol(&mut self, protocol: Protocol) -> &mut Self {
        self.config.protocol = protocol;
        self
    }

    pub fn connections(&mut self, connections: usize) -> &mut Self {
        self.config.connections = connections;
        self
    }

    pub fn streams(&mut self, streams: usize) -> &mut Self {
        self.config.streams = streams;
        self
    }

    pub fn warm_up_time(&mut self, warm_up_time: Duration) -> &mut Self {
        self.config.warm_up_time = warm_up_time;
        self
    }

    pub fn load(&mut self, load: Load) -> &mut Self {
        self.config.load = load;
        self
    }

    pub fn bench_function<F: FnMut(&mut Bencher)>(
        &mut self,
        id: impl Into<BenchmarkId>,
        f: F,
    ) -> &mut Self {
        let BenchmarkId {
            function,
            parameter,
        } = id.into();
        let ids = [Some(self.name.clone()), function, parameter];
        self.criterion
            .run(&self.config, ids.into_iter().flatten().collect(), f);
        self
    }

    pub fn bench_with_input<I: ?Sized, F: FnMut(&mut Bencher, &I)>(
        &mut self,
        id: BenchmarkId,
        input: &I,
        mut f: F,
    ) -> &mut Self {
        self.bench_function(id, |b| f(b, input))
    }

    pub fn finish(self) {}
}

fn measure(db: &Path, total: f64, vus_max: usize, errors: BTreeMap<String, u64>) -> Measurement {
    let mut durations = Vec::new();
    let mut status_codes = BTreeMap::new();
    let mut data = 0;
    if db.exists() {
        let conn = rusqlite::Connection::open(db).expect("gumo: cannot open the oha database");
        let mut stmt = conn
            .prepare("SELECT duration, status, len_bytes FROM oha")
            .expect("gumo: cannot query the oha database");
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, f64>(0)?, row.get(1)?, row.get::<_, i64>(2)?))
            })
            .expect("gumo: cannot query the oha database");
        for row in rows {
            let (duration, status, len) = row.expect("gumo: cannot read an oha database row");
            durations.push(duration * 1000.0);
            *status_codes.entry(status).or_default() += 1;
            data += len as u64;
        }
    }
    durations.sort_by(f64::total_cmp);

    let errored: u64 = errors.values().sum();
    let count = durations.len() as u64 + errored;
    let failed = errored
        + status_codes
            .iter()
            .filter(|&(&status, _)| status >= 400)
            .map(|(_, &n)| n)
            .sum::<u64>();
    Measurement {
        metrics: Metrics {
            http_req_duration: trend(&durations),
            http_reqs: Counter {
                count,
                rate: count as f64 / total,
            },
            data_received: Counter {
                count: data,
                rate: data as f64 / total,
            },
            http_req_failed: Rate {
                passes: failed,
                fails: count - failed,
                value: failed as f64 / count as f64,
            },
            vus_max,
        },
        status_codes,
        errors,
    }
}

fn trend(sorted: &[f64]) -> Trend {
    let (Some(&min), Some(&max)) = (sorted.first(), sorted.last()) else {
        return Trend::default();
    };
    Trend {
        avg: sorted.iter().sum::<f64>() / sorted.len() as f64,
        min,
        med: percentile(sorted, 0.5),
        max,
        p90: percentile(sorted, 0.9),
        p95: percentile(sorted, 0.95),
        p99: percentile(sorted, 0.99),
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let rank = p * (sorted.len() - 1) as f64;
    let low = rank.floor() as usize;
    let high = rank.ceil() as usize;
    sorted[low] + (sorted[high] - sorted[low]) * (rank - low as f64)
}

fn relative_change(new: &Value, base: &Value) -> Option<Value> {
    if let (Some(new), Some(base)) = (new.as_f64(), base.as_f64()) {
        return Some((new / base - 1.0).into());
    }
    let (Some(new), Some(base)) = (new.as_object(), base.as_object()) else {
        return None;
    };
    let change = new
        .iter()
        .filter_map(|(key, new)| Some((key.clone(), relative_change(new, base.get(key)?)?)))
        .collect();
    Some(Value::Object(change))
}

fn print_measurement(full_id: &str, measurement: &Measurement, change: Option<&Value>) {
    let Metrics {
        http_req_duration: Trend {
            min, med, p95, max, ..
        },
        http_reqs: Counter { count, rate },
        http_req_failed,
        ..
    } = &measurement.metrics;
    println!("{full_id}");
    println!(
        "{:24}duration: [min {min:.3} ms, med {med:.3} ms, p95 {p95:.3} ms, max {max:.3} ms]",
        ""
    );
    println!(
        "{:24}http_reqs: {count} ({rate:.1}/s), failed: {:.2}%",
        "",
        http_req_failed.value * 100.0
    );
    if let Some(change) = change {
        let percent = |metric: &str, key: &str| {
            change["metrics"][metric][key].as_f64().unwrap_or(f64::NAN) * 100.0
        };
        println!(
            "{:24}change: [med {:+.2}%, p95 {:+.2}%, rate {:+.2}%]",
            "",
            percent("http_req_duration", "med"),
            percent("http_req_duration", "p(95)"),
            percent("http_reqs", "rate"),
        );
    }
}

fn write_json(path: &Path, value: &Value) {
    let json = serde_json::to_vec_pretty(value).expect("gumo: cannot serialize JSON");
    fs::write(path, json).unwrap_or_else(|e| panic!("gumo: cannot write {}: {e}", path.display()));
}

fn duration_arg(duration: Duration) -> String {
    format!("{}us", duration.as_micros())
}

fn path_arg(path: &Path) -> String {
    path.to_str()
        .expect("gumo: output path is not UTF-8")
        .to_string()
}

fn make_filename_safe(id: &str) -> String {
    id.replace(['?', '"', '/', '\\', '*', '<', '>', ':', '|', '^'], "_")
}

fn cargo_target_directory() -> PathBuf {
    #[derive(Deserialize)]
    struct Metadata {
        target_directory: PathBuf,
    }

    if let Some(dir) = std::env::var_os("CARGO_TARGET_DIR") {
        return PathBuf::from(dir);
    }
    std::env::var_os("CARGO")
        .and_then(|cargo| {
            let output = Command::new(cargo)
                .args(["metadata", "--format-version", "1", "--no-deps"])
                .output()
                .ok()?;
            serde_json::from_slice::<Metadata>(&output.stdout).ok()
        })
        .map_or_else(
            || PathBuf::from("target"),
            |metadata| metadata.target_directory,
        )
}

#[macro_export]
macro_rules! criterion_group {
    (name = $name:ident; config = $config:expr; targets = $($target:path),+ $(,)?) => {
        pub fn $name() {
            let mut criterion: $crate::Criterion = $config.configure_from_args();
            $( $target(&mut criterion); )+
        }
    };
    ($name:ident, $($target:path),+ $(,)?) => {
        $crate::criterion_group!(name = $name; config = $crate::Criterion::default(); targets = $($target),+);
    };
}

#[macro_export]
macro_rules! criterion_main {
    ($($group:path),+ $(,)?) => {
        fn main() {
            $( $group(); )+
        }
    };
}
