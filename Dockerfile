# Build with musl for a static binary
#FROM rust:1.81-alpine AS builder

FROM docker.io/blackdex/rust-musl:x86_64-musl as build

RUN apt-get update && apt-get upgrade -y && apt-get install ca-certificates -y

WORKDIR /app

COPY . .

RUN mv /app/binance.com.pem /usr/local/share/ca-certificates && update-ca-certificates

# If you want to use PostgreSQL v15 add and uncomment the following ENV
# ENV PQ_LIB_DIR="/usr/local/musl/pq15/lib"

RUN cargo build --release

FROM scratch

WORKDIR /
COPY --from=build /app/target/x86_64-unknown-linux-musl/release/s9_binance_adapter /s9_binance_adapter
ENTRYPOINT ["/s9_binance_adapter"]
#CMD ["/my-application-name"]

##RUN apt-get update && apt-get install -y \
##    pkg-config \
##    libssl-dev \
##    gcc-x86-64-linux-gnu \
##    && rm -rf /var/lib/apt/lists/*
##RUN rustup target add x86_64-unknown-linux-musl
#RUN rustup target add x86_64-unknown-linux-gnu
#
#RUN apk add --no-cache musl-dev openssl-dev openssl-libs-static
#
#ENV OPENSSL_STATIC=1
#
## Set the linker for cross-compilation
##ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=x86_64-linux-gnu-gcc
#
#WORKDIR /app
#COPY . .
##RUN cargo build --release --target x86_64-unknown-linux-musl
#RUN cargo build --release --target x86_64-unknown-linux-gnu
#
#RUN ls /app/target/
#
## Minimal final image
#FROM scratch
#COPY --from=builder /app/target/x86_64-unknown-linux-gnu/release/s9_binance_adapter /s9_binance_adapter
#ENTRYPOINT ["/s9_binance_adapter"]