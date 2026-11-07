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

## Command-line tool

`gumo URL` ramps the request rate from 0 to 10,000 requests per second and plots, live in the
terminal, the measured p50 and p99 latency against the request rate:

```sh
cargo run --release -p gumo-cli -- http://localhost:3000/health
cargo run --release -p gumo-cli -- --help   # --max-rate, --duration, --connections, --http2, ...
```
