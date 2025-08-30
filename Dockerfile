# Build with musl for a static binary
FROM rust:1.81-slim AS builder

RUN apt-get update && apt-get install -y \
    musl-tools \
    musl-dev \
    gcc-x86-64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add x86_64-unknown-linux-musl

# Set the linker for cross-compilation
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=x86_64-linux-gnu-gcc

WORKDIR /app
COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl

# Minimal final image
FROM scratch
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/s9_binance_adapter /s9_binance_adapter
ENTRYPOINT ["/s9_binance_adapter"]