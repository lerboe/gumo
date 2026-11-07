use std::collections::BTreeMap;
use std::fmt::Display;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::mpsc;
use std::time::Duration;

use bytes::Bytes;
use http::Request;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use http;

mod client;
mod engine;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
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

#[derive(Clone, Debug, Serialize)]
struct Config {
    target: String,
    protocol: Protocol,
    connections: usize,
    streams: usize,
    warm_up_time: Duration,
    timeout: Duration,
    load: Load,
}

impl Config {
    fn vus_max(&self) -> usize {
        match self.protocol {
            Protocol::Http1 => self.connections,
            Protocol::Http2 => self.connections * self.streams,
        }
    }
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
#[serde(default)]
pub struct Metrics {
    pub http_req_duration: Trend,
    pub http_reqs: Counter,
    pub data_received: Counter,
    pub http_req_failed: Rate,
    /// Scheduled requests that were not sent because every virtual user was busy.
    pub dropped_iterations: Counter,
    pub vus_max: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Measurement {
    pub metrics: Metrics,
    pub status_codes: BTreeMap<u16, u64>,
    pub errors: BTreeMap<String, u64>,
}

/// One request of the measured phase, reported live by [`Criterion::measure`].
#[derive(Clone, Debug)]
pub struct Sample {
    /// When the request was sent (or, if dropped, scheduled), relative to the start of the
    /// measured phase.
    pub offset: Duration,
    pub outcome: SampleOutcome,
}

#[derive(Clone, Debug)]
pub enum SampleOutcome {
    Response {
        status: u16,
        duration: Duration,
    },
    Error(String),
    /// The request was not sent because every virtual user was busy.
    Dropped,
}

pub struct Bencher<'a> {
    config: &'a Config,
    runtime: &'a tokio::runtime::Runtime,
    record: Option<engine::Record>,
}

impl Bencher<'_> {
    /// Runs the benchmark, calling `routine` once for every request that is sent.
    ///
    /// Request URIs are paths (`/users?id=1`), relative to the target address.
    pub fn iter<B: Into<Bytes>, F: FnMut() -> Request<B>>(&mut self, mut routine: F) {
        let routine = || routine().map(Into::into);
        self.record = Some(
            self.runtime
                .block_on(engine::run(self.config, routine, None)),
        );
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
                timeout: Duration::from_secs(60),
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

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
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

    /// Runs a single load test against the target, outside of any benchmark: nothing is written
    /// to disk or printed. Every request of the measured phase is sent to `observer` as it
    /// completes, which allows following the test live.
    pub fn measure<B: Into<Bytes>, F: FnMut() -> Request<B>>(
        &self,
        mut routine: F,
        observer: Option<mpsc::Sender<Sample>>,
    ) -> Measurement {
        assert!(!self.config.target.is_empty(), "gumo: no target address");
        let routine = || routine().map(Into::into);
        let record = self
            .runtime
            .block_on(engine::run(&self.config, routine, observer));
        measure(record, self.config.vus_max())
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

        let mut bencher = Bencher {
            config,
            runtime: &self.runtime,
            record: None,
        };
        f(&mut bencher);
        let Some(record) = bencher.record else {
            panic!("gumo: {full_id} did not call Bencher::iter");
        };
        let measurement = measure(record, config.vus_max());
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

    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        self.config.timeout = timeout;
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

fn measure(record: engine::Record, vus_max: usize) -> Measurement {
    let engine::Record {
        mut durations,
        status_codes,
        errors,
        data_received,
        dropped,
        elapsed,
    } = record;
    durations.sort_by(f64::total_cmp);
    let total = elapsed.as_secs_f64();
    let counter = |count: u64| Counter {
        count,
        rate: if total > 0.0 {
            count as f64 / total
        } else {
            0.0
        },
    };

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
            http_reqs: counter(count),
            data_received: counter(data_received),
            http_req_failed: Rate {
                passes: failed,
                fails: count - failed,
                value: if count > 0 {
                    failed as f64 / count as f64
                } else {
                    0.0
                },
            },
            dropped_iterations: counter(dropped),
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
        dropped_iterations,
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
    if dropped_iterations.count > 0 {
        println!(
            "{:24}dropped_iterations: {} ({:.1}/s)",
            "", dropped_iterations.count, dropped_iterations.rate
        );
    }
    for (error, count) in &measurement.errors {
        println!("{:24}error: {error} ({count})", "");
    }
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
