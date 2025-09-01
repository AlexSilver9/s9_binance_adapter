FROM ubuntu:latest as builder

RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    ca-certificates curl build-essential pkg-config libssl-dev

WORKDIR /app

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN . "/root/.cargo/env"

COPY . /app

RUN /root/.cargo/bin/cargo build --release

RUN find / -type f -name "s9_binance_adapter"


FROM ubuntu:latest

RUN apt-get update -y && apt-get install -y ca-certificates

WORKDIR /
COPY --from=builder /app/target/release/s9_binance_adapter /app/s9_binance_adapter
ENTRYPOINT ["/app/s9_binance_adapter"]
