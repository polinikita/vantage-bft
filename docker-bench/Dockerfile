# syntax=docker/dockerfile:1
# Build from the repository root. Cached dependency layers speed source-only rebuilds.
FROM rust:1.95-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
        clang libclang-dev lld cmake build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ENV CARGO_BUILD_JOBS=4

# Copy manifests before sources to preserve the dependency cache.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY primary/Cargo.toml primary/Cargo.toml
COPY node/Cargo.toml node/Cargo.toml
COPY store/Cargo.toml store/Cargo.toml
COPY crypto/Cargo.toml crypto/Cargo.toml
COPY worker/Cargo.toml worker/Cargo.toml
COPY network/Cargo.toml network/Cargo.toml
COPY config/Cargo.toml config/Cargo.toml
COPY metrics/Cargo.toml metrics/Cargo.toml

# Compile dependencies against stub crate sources.
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

# Copy sources in dependency order. Touch files to invalidate stub artifacts.
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

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        iproute2 iptables curl ca-certificates bash iputils-ping \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/node /usr/local/bin/node
COPY --from=builder /usr/local/bin/benchmark_client /usr/local/bin/benchmark_client
COPY docker-bench/entrypoint.sh /usr/local/bin/entrypoint.sh
# Remote benchmark entrypoint for non-contiguous private addresses.
COPY docker-bench/wanbench-entrypoint.sh /usr/local/bin/wanbench-entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/wanbench-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
