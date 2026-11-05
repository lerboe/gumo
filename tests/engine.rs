use std::collections::HashSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use gumo::http::{Request, Response, Version};
use gumo::{Criterion, Load, Measurement, Protocol};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;

#[derive(Default)]
struct Seen {
    connections: AtomicUsize,
    paths: Mutex<Vec<String>>,
    versions: Mutex<HashSet<Version>>,
}

/// Serves HTTP/1 and HTTP/2 (prior knowledge); `/slow` answers after 100 ms.
fn server() -> (SocketAddr, Arc<Seen>) {
    let seen = Arc::new(Seen::default());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let state = seen.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                state.connections.fetch_add(1, Ordering::SeqCst);
                let state = state.clone();
                let service = service_fn(move |request: Request<Incoming>| {
                    let state = state.clone();
                    async move {
                        let path = request.uri().path().to_string();
                        state.versions.lock().unwrap().insert(request.version());
                        state.paths.lock().unwrap().push(path.clone());
                        if path == "/slow" {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from("ok"))))
                    }
                });
                tokio::spawn(async move {
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
    });
    (address, seen)
}

fn summary(id: &str) -> Measurement {
    let path = format!(
        "{}/target/gumo/{id}/new/summary.json",
        env!("CARGO_MANIFEST_DIR")
    );
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn criterion(address: SocketAddr) -> Criterion {
    Criterion::default()
        .target(format!("http://{address}"))
        .warm_up_time(Duration::ZERO)
        .load(Load::ConstantVus {
            duration: Duration::from_millis(500),
        })
}

#[test]
fn routine_runs_per_request() {
    let (address, seen) = server();
    let mut calls = 0;
    criterion(address)
        .connections(2)
        .bench_function("engine-routine", |b| {
            b.iter(|| {
                calls += 1;
                Request::get(format!("/n/{calls}")).body("").unwrap()
            })
        });
    let metrics = summary("engine-routine").metrics;
    let paths = seen.paths.lock().unwrap();
    assert!(metrics.http_reqs.count > 10, "{metrics:?}");
    assert_eq!(metrics.http_reqs.count, calls as u64);
    assert_eq!(paths.len(), calls);
    assert_eq!(paths.iter().collect::<HashSet<_>>().len(), calls);
    assert_eq!(seen.connections.load(Ordering::SeqCst), 2);
}

#[test]
fn http2_multiplexes_streams() {
    let (address, seen) = server();
    criterion(address)
        .protocol(Protocol::Http2)
        .connections(2)
        .streams(4)
        .bench_function("engine-http2", |b| {
            b.iter(|| Request::get("/").body("").unwrap())
        });
    let metrics = summary("engine-http2").metrics;
    assert!(metrics.http_reqs.count > 10, "{metrics:?}");
    assert_eq!(metrics.http_req_failed.passes, 0);
    assert_eq!(metrics.vus_max, 8);
    assert_eq!(seen.connections.load(Ordering::SeqCst), 2);
    assert_eq!(
        *seen.versions.lock().unwrap(),
        HashSet::from([Version::HTTP_2])
    );
}

#[test]
fn busy_virtual_users_drop_arrivals() {
    let (address, _) = server();
    criterion(address)
        .connections(1)
        .load(Load::ConstantArrivalRate {
            rate: 100.0,
            duration: Duration::from_secs(1),
        })
        .bench_function("engine-dropped", |b| {
            b.iter(|| Request::get("/slow").body("").unwrap())
        });
    let metrics = summary("engine-dropped").metrics;
    let sent = metrics.http_reqs.count;
    let dropped = metrics.dropped_iterations.count;
    assert!((5..=15).contains(&sent), "{metrics:?}");
    assert_eq!(sent + dropped, 100, "{metrics:?}");
}
