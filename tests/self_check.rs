use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use gumo::http::Request;
use gumo::{Criterion, Load, Measurement, Stage};

fn serve(stream: TcpStream) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut writer = stream;
    let mut line = String::new();
    loop {
        line.clear();
        let Ok(n) = reader.read_line(&mut line) else {
            return;
        };
        if n == 0 {
            return;
        }
        if line == "\r\n" {
            let Ok(()) = writer.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok") else {
                return;
            };
        }
    }
}

fn summary(id: &str) -> Measurement {
    let path = format!(
        "{}/target/gumo/{id}/new/summary.json",
        env!("CARGO_MANIFEST_DIR")
    );
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

#[test]
fn bench_writes_summary() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let stream = stream.unwrap();
            thread::spawn(move || serve(stream));
        }
    });

    let mut c = Criterion::default()
        .target(format!("http://{address}"))
        .connections(4)
        .warm_up_time(Duration::from_millis(200))
        .load(Load::ConstantArrivalRate {
            rate: 200.0,
            duration: Duration::from_secs(1),
        });
    let request = || {
        Request::get("/hello")
            .header("x-gumo", "1")
            .body("")
            .unwrap()
    };
    for _ in 0..2 {
        c.bench_function("self-check-constant", |b| b.iter(request));
    }
    let constant = summary("self-check-constant");
    let metrics = &constant.metrics;
    assert!(
        (150..=250).contains(&metrics.http_reqs.count),
        "{metrics:?}"
    );
    assert_eq!(metrics.http_req_failed.passes, 0);
    assert_eq!(constant.status_codes[&200], metrics.http_reqs.count);
    assert_eq!(metrics.data_received.count, 2 * metrics.http_reqs.count);
    let duration = &metrics.http_req_duration;
    assert!(
        duration.min <= duration.med
            && duration.med <= duration.p95
            && duration.p95 <= duration.max
    );
    let change = format!(
        "{}/target/gumo/self-check-constant/change/summary.json",
        env!("CARGO_MANIFEST_DIR")
    );
    assert!(std::path::Path::new(&change).exists());

    let mut group = c.benchmark_group("self-check-ramp");
    group
        .warm_up_time(Duration::ZERO)
        .load(Load::RampingArrivalRate {
            start_rate: 0.0,
            stages: vec![
                Stage {
                    duration: Duration::from_secs(1),
                    target: 100.0,
                },
                Stage {
                    duration: Duration::from_secs(1),
                    target: 0.0,
                },
            ],
        });
    group.bench_function("get", |b| b.iter(request));
    group.finish();
    let ramp = summary("self-check-ramp/get").metrics;
    assert!((80..=120).contains(&ramp.http_reqs.count), "{ramp:?}");
    assert!((40.0..=60.0).contains(&ramp.http_reqs.rate), "{ramp:?}");
}
