FROM rust:1.94-alpine AS builder

WORKDIR /opt/app

COPY . .

RUN cargo build --release

FROM alpine:3.23

ENV RUST_LOG=INFO

WORKDIR /opt/app

COPY --from=builder /opt/app/target/release/nats-flux .

RUN addgroup -g 1000 -S xxx &&\
    adduser -u 1000 -S -g xxx xxx &&\
    chown -R xxx:xxx /opt/app

USER xxx

ENTRYPOINT ["/opt/app/nats-flux"]
