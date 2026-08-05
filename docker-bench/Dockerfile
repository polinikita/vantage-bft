# syntax=docker/dockerfile:1
#
# docker-bench build vehicle: one image runs a primary + one worker + one benchmark
# client per container (see entrypoint.sh). Build from the REPO ROOT so the workspace's
# Cargo.tomls and every crate directory are in the context:
#   DOCKER_BUILDKIT=1 docker build -f docker-bench/Dockerfile -t vantage-docker-bench:latest .
# (run.sh does exactly this.)
#
# Caching design (owner requirement -- see docker-bench/README.md):
#   1. BuildKit cache mounts on the cargo registry/git dirs AND the shared cargo
#      workspace `target/` dir -- persist across builds on this host independently of
#      Docker's own layer cache, so cargo's normal incremental compilation carries
#      forward even when an upstream layer was invalidated.
#   2. A dependency-precompile layer: only the 9 Cargo.toml files are copied first, a
#      trivial stub `src/` tree is generated, and `cargo build` runs against THAT --
#      compiling every external dependency (tokio, rocksdb, axum, ...) without any real
#      source present. This layer's own Docker cache key depends only on
#      Cargo.toml/Cargo.lock, so it (and the potentially slow rocksdb/bindgen/clang
#      dependency build it performs) is skipped by Docker entirely whenever only
#      application source changed.
#   3. Only then are the real `src/` trees copied in, one COPY per crate, each
#      immediately followed by a `touch` of exactly what that COPY just wrote. The
#      touch is the counterintuitive-but-necessary part: BuildKit's COPY (unlike the
#      legacy docker builder) preserves each file's ORIGINAL host mtime instead of
#      stamping "now" -- so without it, a crate's real source (last edited, say,
#      minutes before this build started) can look OLDER to Cargo's mtime-based
#      fingerprinting than the dependency-precompile layer's own stub build of that
#      same crate (which necessarily finished more recently), and Cargo then treats
#      the stub artifact as still up to date and silently skips recompiling the real
#      source entirely -- confirmed the hard way: without this, the image's `node`
#      binary was the one-line stub, `--help` printed nothing.
#
#      The 8 COPY+touch pairs are ordered dependencies-before-dependents (crypto,
#      store, metrics, config, network, primary, worker, node) rather than
#      alphabetically or arbitrarily. This is load-bearing, not cosmetic: Docker's
#      layer cache is a strict linear chain within one stage, so once any one
#      COPY+touch pair is invalidated, EVERY pair after it in the Dockerfile is also
#      forced to re-run (confirmed empirically -- an unrelated, byte-identical file
#      copied two instructions later still shows as a cache miss, not a hit), even
#      though nothing about ITS OWN input changed. Ordering the crates topologically
#      turns that forced-forward cascade into exactly "this crate plus its transitive
#      reverse-dependents" instead of "everything, regardless of what changed": e.g.
#      touching `primary/src/lib.rs` cascades only to `worker` and `node` (both
#      actually depend on `primary`); `crypto`/`store`/`metrics`/`config`/`network`
#      (all ordered before `primary`) are untouched and stay real Docker cache hits.
#      The one residual imprecision is between same-tier siblings with no dependency
#      relationship to each other (e.g. `store` is listed after `crypto` purely to
#      have *a* fixed order, not because it depends on it) -- touching one can
#      spuriously cascade into recompiling the other. That is strictly conservative
#      (never skips a recompile that was actually needed, only occasionally does one
#      more than strictly necessary), and either way touches only OUR OWN crates,
#      never the external dependency graph.
#
# gnu/bookworm, NOT musl: RocksDB (via librocksdb-sys/bindgen) is fragile under musl;
# glibc is the only target this project builds against anywhere else. Matches the
# existing root Dockerfile's own toolchain image exactly (rust-toolchain.toml pins
# 1.95.0).
FROM rust:1.95-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
        clang libclang-dev lld cmake build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ENV CARGO_BUILD_JOBS=4

# ---- manifests only: this layer's cache key ignores every application source file ----
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY primary/Cargo.toml primary/Cargo.toml
COPY node/Cargo.toml node/Cargo.toml
COPY store/Cargo.toml store/Cargo.toml
COPY crypto/Cargo.toml crypto/Cargo.toml
COPY worker/Cargo.toml worker/Cargo.toml
COPY network/Cargo.toml network/Cargo.toml
COPY config/Cargo.toml config/Cargo.toml
COPY metrics/Cargo.toml metrics/Cargo.toml

# ---- dependency precompile against stub sources (see header comment) ----
RUN mkdir -p primary/src store/src crypto/src worker/src network/src config/src metrics/src node/src && \
    for c in primary store crypto worker network config metrics; do \
        printf '// stub for dependency precompile\n' > "$c/src/lib.rs"; \
    done && \
    printf 'fn main() {}\n' > node/src/main.rs && \
    printf 'fn main() {}\n' > node/src/benchmark_client.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=vantage-cargo-registry \
    --mount=type=cache,target=/usr/local/cargo/git,id=vantage-cargo-git \
    --mount=type=cache,target=/app/target,id=vantage-cargo-target,sharing=locked \
    cargo build --manifest-path node/Cargo.toml --release --locked --features benchmark

# ---- real sources: COPY + touch per crate, dependencies before dependents (see
# header comment for why the order is load-bearing) ----
COPY crypto/src crypto/src
RUN find crypto/src -exec touch {} +
COPY store/src store/src
RUN find store/src -exec touch {} +
COPY metrics/src metrics/src
RUN find metrics/src -exec touch {} +
COPY config/src config/src
RUN find config/src -exec touch {} +
COPY network/src network/src
RUN find network/src -exec touch {} +
COPY primary/src primary/src
RUN find primary/src -exec touch {} +
COPY worker/src worker/src
RUN find worker/src -exec touch {} +
COPY node/src node/src
RUN find node/src -exec touch {} +

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=vantage-cargo-registry \
    --mount=type=cache,target=/usr/local/cargo/git,id=vantage-cargo-git \
    --mount=type=cache,target=/app/target,id=vantage-cargo-target,sharing=locked \
    cargo build --manifest-path node/Cargo.toml --release --locked --features benchmark && \
    cp target/release/node target/release/benchmark_client /usr/local/bin/ && \
    strip /usr/local/bin/node /usr/local/bin/benchmark_client

# ---------------------------------------------------------------------------
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        iproute2 iptables curl ca-certificates bash iputils-ping \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/node /usr/local/bin/node
COPY --from=builder /usr/local/bin/benchmark_client /usr/local/bin/benchmark_client
COPY docker-bench/entrypoint.sh /usr/local/bin/entrypoint.sh
# wan-bench remote entrypoint (arbitrary private IPs; addresses from committee.json
# + env, not the gen.py contiguous NODE_IP_PREFIX scheme). Selected via
# `docker run --entrypoint /usr/local/bin/wanbench-entrypoint.sh` by wan-bench.
COPY docker-bench/wanbench-entrypoint.sh /usr/local/bin/wanbench-entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/wanbench-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
