# syntax=docker/dockerfile:1
#
# Build vehicle for the CI-produced nightly binaries (node + benchmark_client).
# gnu/bookworm, NOT musl -- RocksDB (via librocksdb-sys/bindgen) is fragile
# under musl; glibc is the only target this project builds against anywhere
# else (remote.py's old source-build path installs the same clang/cmake/
# build-essential trio on plain Ubuntu, never musl).
FROM rust:1.95-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
        clang libclang-dev lld cmake build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY primary ./primary
COPY node ./node
COPY store ./store
COPY crypto ./crypto
COPY worker ./worker
COPY network ./network
COPY config ./config
COPY metrics ./metrics
# `--features benchmark` (node/Cargo.toml) is what makes the `benchmark_client`
# `[[bin]]` buildable (required-features) and is the same flag
# benchmark/benchmark/commands.py's CommandMaker.compile() has always used for
# the old remote source-build path -- both binaries `benchmark/benchmark/
# remote.py` launches (`./node`, `./benchmark_client`) come out of this one
# build.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target,sharing=locked \
    (cd node && cargo build --release --features benchmark) && \
    cp target/release/node target/release/benchmark_client /usr/local/bin/ && \
    strip /usr/local/bin/node /usr/local/bin/benchmark_client

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/node /usr/local/bin/node
COPY --from=builder /usr/local/bin/benchmark_client /usr/local/bin/benchmark_client
ENTRYPOINT ["/usr/local/bin/node"]
