# gumo: HTTP load generator benchmarking tool

[![Crates.io][crates-badge]][crates-url]
[![MIT licensed][mit-badge]][mit-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/gumo.svg
[crates-url]: https://crates.io/crates/gumo
[mit-badge]: https://img.shields.io/badge/License-GPL_v3-blue.svg
[mit-url]: LICENSE
[actions-badge]: https://github.com/lerboe/gumo/actions/workflows/ci.yml/badge.svg
[actions-url]: https://github.com/lerboe/gumo/actions/workflows/ci.yml

gumo ([guete morge](https://studyinginswitzerland.com/swiss-greeting/)) is benchmarking tool specifically designed for web servers. Unlike other code performance tools, e.g., [criterion](https://crates.io/crates/criterion), gumo allows you benchmark at a constant load. This is important for web servers in order to quantify high latency artifacts, e.g., the p99 request latency. This is known as the [coordinated omission effect](https://redhatperf.github.io/post/coordinated-omission/). gumo is packaged as a [library][crates-url] and as a command-line tool.

## Command-line tool

`gumo URL` ramps the request rate from 0 to 10,000 requests per second and plots, live in the
terminal, the measured p50 and p99 latency against the request rate:

```sh
cargo run --release -p cli -- http://localhost:3000/health
cargo run --release -p cli -- --help   # --max-rate, --duration, --connections, --http2, ...
```
