# Multi-stage Dockerfile for pyaleph-rs (Aleph Core Node)

# ── Builder stage ─────────────────────────────────────────────
FROM rust:1.83-bookworm AS builder

WORKDIR /build

# Cache dependency compilation
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs \
    && cargo build --release 2>/dev/null || true \
    && rm -rf src

# Build the full project
COPY . .
RUN cargo build --release

# ── Runtime stage ─────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/aleph-core /usr/local/bin/aleph-core

# Copy default config
RUN mkdir -p /etc/aleph /data
COPY config.example.toml /etc/aleph/config.toml

EXPOSE 8080
VOLUME ["/data"]

ENTRYPOINT ["aleph-core"]
CMD ["--config", "/etc/aleph/config.toml"]
