//! Load generation: drives virtual users according to the configured load.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::Request;
use http_body_util::Full;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Instant, sleep_until};

use crate::client::{Client, Outcome, Target};
use crate::{Config, Load, Protocol, Stage};

/// Everything recorded during one measured phase.
#[derive(Default)]
pub(crate) struct Record {
    /// Durations of the requests that got a response, in milliseconds.
    pub(crate) durations: Vec<f64>,
    pub(crate) status_codes: BTreeMap<u16, u64>,
    pub(crate) errors: BTreeMap<String, u64>,
    pub(crate) data_received: u64,
    /// Arrivals that found no idle virtual user.
    pub(crate) dropped: u64,
    pub(crate) elapsed: Duration,
}

impl Record {
    fn add(&mut self, outcome: Outcome) {
        match outcome.result {
            Ok(response) => {
                self.durations.push(outcome.duration.as_secs_f64() * 1000.0);
                *self.status_codes.entry(response.status).or_default() += 1;
                self.data_received += response.body_len;
            }
            Err(error) => *self.errors.entry(error).or_default() += 1,
        }
    }
}

/// A virtual user reporting back: it finished a request (or connected) and is idle again.
struct Done {
    vu: usize,
    outcome: Option<Outcome>,
}

enum Phase {
    /// Every virtual user sends its next request as soon as the last one finished.
    Closed,
    /// Requests arrive on a schedule, independent of how fast they complete.
    Open(Vec<Ramp>),
}

/// A linear change of the arrival rate (requests per second) over `duration`.
#[derive(Clone, Copy)]
struct Ramp {
    duration: f64,
    from: f64,
    to: f64,
}

pub(crate) async fn run<F: FnMut() -> Request<Bytes>>(config: &Config, routine: F) -> Record {
    let mut engine = Engine::start(config, routine).await;
    if !config.warm_up_time.is_zero() {
        engine.phase(Phase::Closed, config.warm_up_time).await;
    }
    let (phase, duration) = match &config.load {
        Load::ConstantVus { duration } => (Phase::Closed, *duration),
        Load::ConstantArrivalRate { rate, duration } => (
            Phase::Open(vec![Ramp {
                duration: duration.as_secs_f64(),
                from: *rate,
                to: *rate,
            }]),
            *duration,
        ),
        Load::RampingArrivalRate { start_rate, stages } => {
            let mut from = *start_rate;
            let ramps = stages
                .iter()
                .map(|&Stage { duration, target }| {
                    let ramp = Ramp {
                        duration: duration.as_secs_f64(),
                        from,
                        to: target,
                    };
                    from = target;
                    ramp
                })
                .collect();
            (
                Phase::Open(ramps),
                stages.iter().map(|stage| stage.duration).sum(),
            )
        }
    };
    engine.phase(phase, duration).await
}

struct Engine<'a, F> {
    target: Arc<Target>,
    routine: F,
    /// One request channel per virtual user.
    vus: Vec<mpsc::Sender<Request<Full<Bytes>>>>,
    idle: VecDeque<usize>,
    done: mpsc::UnboundedReceiver<Done>,
    config: &'a Config,
}

impl<'a, F: FnMut() -> Request<Bytes>> Engine<'a, F> {
    /// Spawns the virtual users and waits until all of them are connected.
    async fn start(config: &'a Config, routine: F) -> Self {
        assert!(config.connections > 0, "gumo: connections must be positive");
        let target = Arc::new(Target::new(&config.target, config.protocol));
        let streams = match config.protocol {
            Protocol::Http1 => 1,
            Protocol::Http2 => {
                assert!(config.streams > 0, "gumo: streams must be positive");
                config.streams
            }
        };
        let (done_tx, done) = mpsc::unbounded_channel();
        let mut vus = Vec::new();
        for _ in 0..config.connections {
            let shared = (config.protocol == Protocol::Http2).then(|| Arc::new(Mutex::new(None)));
            for _ in 0..streams {
                let client = Client::new(target.clone(), shared.clone(), config.timeout);
                let (tx, rx) = mpsc::channel(1);
                tokio::spawn(virtual_user(vus.len(), client, rx, done_tx.clone()));
                vus.push(tx);
            }
        }
        let mut engine = Engine {
            target,
            routine,
            idle: VecDeque::with_capacity(vus.len()),
            vus,
            done,
            config,
        };
        while engine.idle.len() < engine.vus.len() {
            let Done { vu, outcome } = engine.recv().await;
            if let Some(Outcome { result: Err(e), .. }) = outcome {
                panic!("gumo: cannot connect to {}: {e}", engine.config.target);
            }
            engine.idle.push_back(vu);
        }
        engine
    }

    async fn recv(&mut self) -> Done {
        self.done
            .recv()
            .await
            .expect("gumo: all virtual users stopped")
    }

    fn handle(&mut self, done: Done, record: &mut Record) {
        if let Some(outcome) = done.outcome {
            record.add(outcome);
        }
        self.idle.push_back(done.vu);
    }

    /// Hands the next request to an idle virtual user, if there is one.
    fn dispatch(&mut self) -> bool {
        let Some(vu) = self.idle.pop_front() else {
            return false;
        };
        let request = self.target.prepare((self.routine)());
        self.vus[vu]
            .try_send(request)
            .expect("gumo: an idle virtual user is not ready");
        true
    }

    async fn phase(&mut self, phase: Phase, duration: Duration) -> Record {
        let mut record = Record::default();
        let start = Instant::now();
        let deadline = start + duration;
        match phase {
            Phase::Closed => loop {
                if Instant::now() >= deadline {
                    break;
                }
                while self.dispatch() {}
                tokio::select! {
                    done = self.recv() => self.handle(done, &mut record),
                    _ = sleep_until(deadline) => break,
                }
            },
            Phase::Open(ramps) => {
                for offset in arrivals(ramps) {
                    let sleep = sleep_until(start + offset);
                    tokio::pin!(sleep);
                    loop {
                        tokio::select! {
                            done = self.recv() => self.handle(done, &mut record),
                            _ = &mut sleep => break,
                        }
                    }
                    while let Ok(done) = self.done.try_recv() {
                        self.handle(done, &mut record);
                    }
                    if !self.dispatch() {
                        record.dropped += 1;
                    }
                }
                let sleep = sleep_until(deadline);
                tokio::pin!(sleep);
                loop {
                    tokio::select! {
                        done = self.recv() => self.handle(done, &mut record),
                        _ = &mut sleep => break,
                    }
                }
            }
        }
        // Requests started before the deadline still count.
        while self.idle.len() < self.vus.len() {
            let done = self.recv().await;
            self.handle(done, &mut record);
        }
        record.elapsed = start.elapsed();
        record
    }
}

async fn virtual_user(
    vu: usize,
    mut client: Client,
    mut requests: mpsc::Receiver<Request<Full<Bytes>>>,
    done: mpsc::UnboundedSender<Done>,
) {
    let outcome = client.connect().await.err().map(|e| Outcome {
        duration: Duration::ZERO,
        result: Err(e),
    });
    if done.send(Done { vu, outcome }).is_err() {
        return;
    }
    while let Some(request) = requests.recv().await {
        let outcome = Some(client.send(request).await);
        if done.send(Done { vu, outcome }).is_err() {
            return;
        }
    }
}

/// Offsets from the phase start at which requests arrive.
///
/// The `n`-th request (counting from 0) arrives when the integral of the arrival rate reaches
/// `n + 1/2`, which spaces a constant rate evenly and sends nothing at a rate of zero.
fn arrivals(ramps: Vec<Ramp>) -> impl Iterator<Item = Duration> {
    let mut ramps = ramps.into_iter();
    let mut ramp = ramps.next();
    // Offset and integrated arrivals at the start of the current ramp.
    let mut start = 0.0;
    let mut arrived = 0.0;
    let mut n = 0u64;
    std::iter::from_fn(move || {
        loop {
            let Ramp { duration, from, to } = ramp?;
            let from = from.max(0.0);
            let to = to.max(0.0);
            // Solve from * t + slope / 2 * t^2 = remaining for t.
            let remaining = n as f64 + 0.5 - arrived;
            let slope = (to - from) / duration;
            let discriminant = from * from + 2.0 * slope * remaining;
            let t = if remaining <= 0.0 {
                0.0
            } else if discriminant < 0.0 || from + discriminant.sqrt() <= 0.0 {
                f64::INFINITY
            } else {
                // Numerically stable form of (-from + sqrt(discriminant)) / slope.
                2.0 * remaining / (from + discriminant.sqrt())
            };
            if t < duration {
                n += 1;
                return Some(Duration::from_secs_f64(start + t));
            }
            start += duration;
            arrived += (from + to) / 2.0 * duration;
            ramp = ramps.next();
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(ramps: &[(f64, f64, f64)]) -> usize {
        let ramps = ramps
            .iter()
            .map(|&(duration, from, to)| Ramp { duration, from, to })
            .collect();
        let offsets: Vec<_> = arrivals(ramps).collect();
        assert!(offsets.windows(2).all(|w| w[0] <= w[1]));
        offsets.len()
    }

    #[test]
    fn constant_rate() {
        assert_eq!(count(&[(2.0, 100.0, 100.0)]), 200);
        assert_eq!(count(&[(2.0, 0.0, 0.0)]), 0);
    }

    #[test]
    fn ramps() {
        assert_eq!(count(&[(1.0, 0.0, 100.0), (1.0, 100.0, 0.0)]), 100);
        assert_eq!(count(&[(1.0, 0.0, 0.0), (1.0, 0.0, 10.0)]), 5);
        assert_eq!(count(&[(4.0, 50.0, 0.0)]), 100);
    }

    #[test]
    fn zero_length_ramp() {
        assert_eq!(count(&[(0.0, 0.0, 100.0), (1.0, 100.0, 100.0)]), 100);
    }
}
