//! Benchmarks the echo server with gumo: `cargo bench -p example [FILTER]`.

use std::net::SocketAddr;
use std::time::Duration;

use gumo::http::Request;
use gumo::{BenchmarkId, Criterion, Load, Protocol, Stage, criterion_group, criterion_main};

/// Starts the echo server on a free port in a background runtime.
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
            example::serve(listener).await.expect("server failed");
        });
    });
    address
}

/// Closed model: every connection sends its next request as soon as the last one is done.
fn echo_body_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("echo");
    for size in [0, 1024, 64 * 1024] {
        let body = vec![b'x'; size];
        group.bench_with_input(BenchmarkId::new("post", size), &body, |b, body| {
            b.iter(|| {
                Request::post("/echo")
                    .header("content-type", "application/octet-stream")
                    .body(body.clone())
                    .unwrap()
            })
        });
    }
    group.finish();
}

/// The same load over HTTP/1.1 and over HTTP/2, with 8 streams per connection.
fn protocols(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol");
    group.connections(8).streams(8);
    for (name, protocol) in [("http1", Protocol::Http1), ("http2", Protocol::Http2)] {
        group.protocol(protocol).bench_function(name, |b| {
            b.iter(|| Request::get("/health").body("").unwrap())
        });
    }
    group.finish();
}

/// Open model: a fixed arrival rate, and a ramp up and back down. A fresh request per call.
fn arrival_rate(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrival_rate");
    let mut n = 0u64;
    group.load(Load::ConstantArrivalRate {
        rate: 2_000.0,
        duration: Duration::from_secs(5),
    });
    group.bench_function("constant", |b| {
        b.iter(|| {
            n += 1;
            Request::post("/echo").body(format!("request {n}")).unwrap()
        })
    });
    group.load(Load::RampingArrivalRate {
        start_rate: 0.0,
        stages: vec![
            Stage {
                duration: Duration::from_secs(3),
                target: 5_000.0,
            },
            Stage {
                duration: Duration::from_secs(3),
                target: 0.0,
            },
        ],
    });
    group.bench_function("ramp", |b| {
        b.iter(|| Request::get("/health").body("").unwrap())
    });
    group.finish();
}

fn config() -> Criterion {
    Criterion::default()
        .target(format!("http://{}", start_server()))
        .connections(16)
        .warm_up_time(Duration::from_secs(1))
        .load(Load::ConstantVus {
            duration: Duration::from_secs(5),
        })
}

criterion_group! {
    name = benches;
    config = config();
    targets = echo_body_size, protocols, arrival_rate
}
criterion_main!(benches);
