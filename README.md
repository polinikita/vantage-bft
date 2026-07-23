# Vantage + Autobahn

[![license](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](LICENSE)

This repository is a shared Rust implementation of two BFT protocols — **Autobahn** and
**Vantage** — built on a common substrate (network, store, config, worker, benchmark
client, and fabric harness) so they can be evaluated head-to-head under an identical setup.

It is a fork of the Autobahn SOSP'24 artifact
([neilgiri/autobahn-artifact](https://github.com/neilgiri/autobahn-artifact), branch
`autobahn`), which is licensed Apache-2.0; the upstream `LICENSE` is retained.

- **Plan and status:** see [`IMPLEMENTATION-PLAN.md`](IMPLEMENTATION-PLAN.md) for the
  phased roadmap (dependency modernization, multi-protocol layout, the Vantage data/consensus
  planes, evaluation) and the current gates.
- **Benchmark harness:** see [`benchmark/`](benchmark/) for the Python fabric harness used to
  run local and distributed experiments.

## License

This software is licensed as [Apache 2.0](LICENSE).
