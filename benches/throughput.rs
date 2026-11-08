//! How many requests gumo sends per second from a single thread: `cargo bench --bench throughput`.
//!
//! The target is an in-process hyper server running on its own multi-threaded runtime, so that
//! the server is not the bottleneck. Every sample opens fresh connections and then sends
//! exactly as many requests as criterion asks for; connecting is not part of the measured time.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use gumo::http::{Request, Response};
use gumo::{Load, Protocol};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;

/// Concurrent requests: HTTP/1.1 connections, or streams on a single HTTP/2 connection.
const CONCURRENCY: [usize; 3] = [1, 16, 64];

/// Serves `ok` over HTTP/1.1 and HTTP/2 (prior knowledge) on a background runtime.
fn start_server() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("cannot bind");
    listener
        .set_nonblocking(true)
        .expect("cannot set non-blocking");
    let address = listener.local_addr().expect("no local address");
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().expect("cannot build runtime");
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).expect("bad listener");
            loop {
                let (stream, _) = listener.accept().await.expect("cannot accept");
                let _ = stream.set_nodelay(true);
                let service = service_fn(|_: Request<Incoming>| async {
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok"))))
                });
                tokio::spawn(async move {
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
    });
    address
}

/// Sends `iterations` requests and returns how long that took, excluding connection setup.
fn send(gumo: &mut Option<gumo::Criterion>, iterations: u64) -> Duration {
    let configured = gumo
        .take()
        .expect("gumo is put back after every sample")
        .load(Load::SharedIterations { iterations });
    let measurement = configured.measure(|| Request::get("/").body("").unwrap(), None);
    *gumo = Some(configured);

    let metrics = measurement.metrics;
    // `passes` counts failed requests, as in k6.
    assert_eq!(
        metrics.http_req_failed.passes, 0,
        "{:?}",
        measurement.errors
    );
    assert_eq!(metrics.http_reqs.count, iterations);
    if metrics.http_reqs.rate == 0.0 {
        return Duration::ZERO;
    }
    Duration::from_secs_f64(metrics.http_reqs.count as f64 / metrics.http_reqs.rate)
}

fn single_thread(c: &mut Criterion) {
    let target = format!("http://{}", start_server());
    let mut group = c.benchmark_group("single_thread");
    group.throughput(Throughput::Elements(1));
    for (name, protocol) in [("http1", Protocol::Http1), ("http2", Protocol::Http2)] {
        for concurrency in CONCURRENCY {
            let (connections, streams) = match protocol {
                Protocol::Http1 => (concurrency, 1),
                Protocol::Http2 => (1, concurrency),
            };
            let mut gumo = Some(
                gumo::Criterion::default()
                    .threads(1)
                    .target(target.as_str())
                    .protocol(protocol)
                    .connections(connections)
                    .streams(streams)
                    .warm_up_time(Duration::ZERO),
            );
            group.bench_function(BenchmarkId::new(name, concurrency), |b| {
                b.iter_custom(|iterations| send(&mut gumo, iterations))
            });
        }
    }
    group.finish();
}

criterion_group!(benches, single_thread);
criterion_main!(benches);
