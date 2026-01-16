FROM rust:1.92 AS builder

WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update \
 && apt-get install -y ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/bully_algorithm /usr/local/bin/bully_algorithm
COPY server-list.json /app/server-list.json

ENV CONFIG_PATH=/app/server-list.json

CMD ["bully_algorithm"]
