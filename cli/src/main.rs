//! `gumo URL`: ramps the request rate against a URL and plots latency against the request rate,
//! live in the terminal.

use std::any::Any;
use std::panic;
use std::process::ExitCode;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use clap::Parser;
use gumo::http::uri::Scheme;
use gumo::http::{Request, Uri};
use gumo::{Criterion, Load, Measurement, Protocol, Sample, SampleOutcome, Stage};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Style, Stylize};
use ratatui::symbols::Marker;
use ratatui::text::Line;
use ratatui::widgets::{Axis, Block, Chart, Dataset, GraphType, LegendPosition};
use ratatui::{DefaultTerminal, Frame};

/// Name of the thread generating the load; its panics are reported after the terminal is restored.
const LOAD_THREAD: &str = "gumo-load";
/// Number of request rate intervals the ramp is split into, one point of the curve each.
const BUCKETS: usize = 100;

/// Ramps the request rate against a URL and plots latency against the request rate, live.
#[derive(Parser)]
#[command(name = "gumo", version)]
struct Args {
    /// The URL to send GET requests to, e.g. http://localhost:8080/health
    url: Uri,
    /// The request rate to ramp up to, in requests per second.
    #[arg(long, default_value_t = 10_000.0)]
    max_rate: f64,
    /// How long the ramp from 0 to the maximum rate takes, in seconds.
    #[arg(long, default_value_t = 60.0)]
    duration: f64,
    /// Number of connections, which bounds the number of concurrent requests.
    #[arg(long, default_value_t = 256)]
    connections: usize,
    /// Use HTTP/2 instead of HTTP/1.1.
    #[arg(long)]
    http2: bool,
    /// Concurrent streams per connection, for HTTP/2.
    #[arg(long, default_value_t = 1)]
    streams: usize,
    /// Request timeout, in seconds.
    #[arg(long, default_value_t = 10.0)]
    timeout: f64,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let Some(authority) = args.url.authority() else {
        eprintln!("gumo: {} has no host", args.url);
        return ExitCode::FAILURE;
    };
    let scheme = args.url.scheme().cloned().unwrap_or(Scheme::HTTP);
    if !(args.max_rate > 0.0 && args.duration > 0.0 && args.timeout > 0.0) {
        eprintln!("gumo: --max-rate, --duration and --timeout must be positive");
        return ExitCode::FAILURE;
    }
    let path: Uri = args
        .url
        .path_and_query()
        .map_or("/", |path| path.as_str())
        .parse()
        .expect("a path of a valid URI is a valid URI");

    let duration = Duration::from_secs_f64(args.duration);
    let criterion = Criterion::default()
        .target(format!("{scheme}://{authority}"))
        .protocol(if args.http2 {
            Protocol::Http2
        } else {
            Protocol::Http1
        })
        .connections(args.connections)
        .streams(args.streams)
        .warm_up_time(Duration::ZERO)
        .timeout(Duration::from_secs_f64(args.timeout))
        .load(Load::RampingArrivalRate {
            start_rate: 0.0,
            stages: vec![Stage {
                duration,
                target: args.max_rate,
            }],
        });

    let (samples, receiver) = mpsc::channel();
    let load = thread::Builder::new()
        .name(LOAD_THREAD.to_string())
        .spawn(move || {
            criterion.measure(
                || Request::get(path.clone()).body("").unwrap(),
                Some(samples),
            )
        })
        .expect("cannot spawn the load thread");

    let mut terminal = ratatui::init();
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        // The load thread's panic (e.g. connection refused) is reported by the main thread.
        if thread::current().name() != Some(LOAD_THREAD) {
            ratatui::restore();
            default_hook(info);
        }
    }));

    let mut app = App {
        url: args.url.to_string(),
        curve: Curve::new(args.max_rate, duration),
        load: Some(load),
        measurement: None,
    };
    let result = app.run(&mut terminal, &receiver);
    ratatui::restore();

    match result {
        Err(error) => {
            eprintln!("gumo: {error}");
            ExitCode::FAILURE
        }
        Ok(Err(panic)) => {
            eprintln!("{}", panic_message(&*panic));
            ExitCode::FAILURE
        }
        Ok(Ok(())) => {
            match &app.measurement {
                Some(measurement) => print_summary(&app.url, measurement),
                None => eprintln!("gumo: aborted"),
            }
            ExitCode::SUCCESS
        }
    }
}

struct App {
    url: String,
    curve: Curve,
    /// The load test, until it finished.
    load: Option<thread::JoinHandle<Measurement>>,
    measurement: Option<Measurement>,
}

impl App {
    /// Renders until the user quits; the inner error is a panic of the load thread.
    fn run(
        &mut self,
        terminal: &mut DefaultTerminal,
        samples: &mpsc::Receiver<Sample>,
    ) -> std::io::Result<thread::Result<()>> {
        loop {
            while let Ok(sample) = samples.try_recv() {
                self.curve.add(sample);
            }
            if self.load.as_ref().is_some_and(|load| load.is_finished()) {
                let load = self.load.take().expect("checked above");
                match load.join() {
                    Ok(measurement) => self.measurement = Some(measurement),
                    Err(panic) => return Ok(Err(panic)),
                }
                // Samples sent right before the end.
                while let Ok(sample) = samples.try_recv() {
                    self.curve.add(sample);
                }
            }
            self.curve.update();
            terminal.draw(|frame| self.render(frame))?;

            if event::poll(Duration::from_millis(100))?
                && let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
                && (matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
                    || (key.code == KeyCode::Char('c')
                        && key.modifiers.contains(KeyModifiers::CONTROL)))
            {
                return Ok(Ok(()));
            }
        }
    }

    fn render(&self, frame: &mut Frame) {
        let curve = &self.curve;
        let [status, chart] =
            Layout::vertical([Constraint::Length(1), Constraint::Fill(1)]).areas(frame.area());

        let state = if self.measurement.is_some() {
            "done, press q to quit".green()
        } else if curve.elapsed.is_zero() {
            "connecting…".yellow()
        } else {
            format!("{:.0}%, q to quit", curve.progress() * 100.0).into()
        };
        frame.render_widget(
            Line::from(vec![
                format!(" GET {}", self.url).bold(),
                format!("  rate {:.0}/s", curve.rate_at(curve.elapsed)).into(),
                format!("  responses {}", curve.responses).into(),
                format!("  errors {}", curve.errors).red(),
                format!("  dropped {}", curve.dropped).magenta(),
                "  ".into(),
                state,
            ]),
            status,
        );

        let p50: Vec<_> = curve.points(|bucket| bucket.p50);
        let p99: Vec<_> = curve.points(|bucket| bucket.p99);
        let max_latency = nice_ceil(p99.iter().map(|&(_, y)| y).fold(0.0, f64::max) * 1.1);
        let datasets = vec![
            Dataset::default()
                .name("p50")
                .marker(Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::new().cyan())
                .data(&p50),
            Dataset::default()
                .name("p99")
                .marker(Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::new().yellow())
                .data(&p99),
        ];
        let x_labels = (0..=4).map(|i| format_rate(curve.max_rate * f64::from(i) / 4.0));
        let y_labels = (0..=4).map(|i| format_latency(max_latency * f64::from(i) / 4.0));
        let chart_widget = Chart::new(datasets)
            .block(Block::bordered().title(" latency vs. request rate ".bold()))
            .x_axis(
                Axis::default()
                    .title("request rate [req/s]")
                    .bounds([0.0, curve.max_rate])
                    .labels(x_labels),
            )
            .y_axis(
                Axis::default()
                    .title("latency")
                    .bounds([0.0, max_latency])
                    .labels(y_labels),
            )
            .legend_position(Some(LegendPosition::TopLeft))
            .hidden_legend_constraints((Constraint::Min(0), Constraint::Min(0)));
        frame.render_widget(chart_widget, chart);
    }
}

/// Latencies grouped by the request rate at the time the requests were sent.
struct Curve {
    max_rate: f64,
    duration: Duration,
    buckets: Vec<Bucket>,
    /// Offset of the latest sample, i.e. how far the ramp has progressed.
    elapsed: Duration,
    responses: u64,
    errors: u64,
    dropped: u64,
}

#[derive(Default)]
struct Bucket {
    /// Response latencies in milliseconds.
    latencies: Vec<f64>,
    /// Whether latencies changed since the percentiles were computed.
    dirty: bool,
    p50: f64,
    p99: f64,
}

impl Curve {
    fn new(max_rate: f64, duration: Duration) -> Self {
        Curve {
            max_rate,
            duration,
            buckets: (0..BUCKETS).map(|_| Bucket::default()).collect(),
            elapsed: Duration::ZERO,
            responses: 0,
            errors: 0,
            dropped: 0,
        }
    }

    fn progress(&self) -> f64 {
        (self.elapsed.as_secs_f64() / self.duration.as_secs_f64()).min(1.0)
    }

    /// The request rate the ramp is at `offset` into the test.
    fn rate_at(&self, offset: Duration) -> f64 {
        self.max_rate * (offset.as_secs_f64() / self.duration.as_secs_f64()).min(1.0)
    }

    fn add(&mut self, sample: Sample) {
        self.elapsed = self.elapsed.max(sample.offset);
        match sample.outcome {
            SampleOutcome::Response { duration, .. } => {
                self.responses += 1;
                let index = (self.rate_at(sample.offset) / self.max_rate * BUCKETS as f64) as usize;
                let bucket = &mut self.buckets[index.min(BUCKETS - 1)];
                bucket.latencies.push(duration.as_secs_f64() * 1000.0);
                bucket.dirty = true;
            }
            SampleOutcome::Error(_) => self.errors += 1,
            SampleOutcome::Dropped => self.dropped += 1,
        }
    }

    /// Recomputes the percentiles of the buckets that got new samples.
    fn update(&mut self) {
        for bucket in self.buckets.iter_mut().filter(|bucket| bucket.dirty) {
            bucket.latencies.sort_unstable_by(f64::total_cmp);
            bucket.p50 = percentile(&bucket.latencies, 0.5);
            bucket.p99 = percentile(&bucket.latencies, 0.99);
            bucket.dirty = false;
        }
    }

    /// One (request rate, latency) point per bucket with responses.
    fn points(&self, latency: impl Fn(&Bucket) -> f64) -> Vec<(f64, f64)> {
        let width = self.max_rate / BUCKETS as f64;
        self.buckets
            .iter()
            .enumerate()
            .filter(|(_, bucket)| !bucket.latencies.is_empty())
            .map(|(i, bucket)| ((i as f64 + 0.5) * width, latency(bucket)))
            .collect()
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let rank = p * (sorted.len() - 1) as f64;
    let low = rank.floor() as usize;
    let high = rank.ceil() as usize;
    sorted[low] + (sorted[high] - sorted[low]) * (rank - low as f64)
}

/// Rounds up to 1, 2 or 5 times a power of ten, for readable axis labels.
fn nice_ceil(value: f64) -> f64 {
    if value <= 0.0 {
        return 1.0;
    }
    let magnitude = 10f64.powf(value.log10().floor());
    [1.0, 2.0, 5.0, 10.0]
        .into_iter()
        .map(|step| step * magnitude)
        .find(|&nice| nice >= value)
        .unwrap_or(10.0 * magnitude)
}

fn format_rate(rate: f64) -> String {
    if rate >= 1000.0 {
        format!("{}k", rate / 1000.0)
    } else {
        format!("{rate:.0}")
    }
}

fn format_latency(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{} s", ms / 1000.0)
    } else {
        format!("{ms} ms")
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("gumo: the load test panicked")
}

fn print_summary(url: &str, measurement: &Measurement) {
    let metrics = &measurement.metrics;
    let duration = &metrics.http_req_duration;
    println!("GET {url}");
    println!(
        "  duration:  min {:.3} ms, med {:.3} ms, p95 {:.3} ms, p99 {:.3} ms, max {:.3} ms",
        duration.min, duration.med, duration.p95, duration.p99, duration.max
    );
    println!(
        "  http_reqs: {} ({:.1}/s), failed: {:.2}%",
        metrics.http_reqs.count,
        metrics.http_reqs.rate,
        metrics.http_req_failed.value * 100.0
    );
    if metrics.dropped_iterations.count > 0 {
        println!(
            "  dropped:   {} ({:.1}/s), raise --connections to send them",
            metrics.dropped_iterations.count, metrics.dropped_iterations.rate
        );
    }
    for (error, count) in &measurement.errors {
        println!("  error:     {error} ({count})");
    }
}
