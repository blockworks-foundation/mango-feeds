# syntax = docker/dockerfile:1.2
# Base image containing all binaries, deployed to gcr.io/mango-markets/mango-geyser-services:latest
FROM rust:1.70.0-bullseye as base
RUN cargo install cargo-chef
RUN rustup component add rustfmt
RUN apt-get update && apt-get install -y clang cmake ssh
WORKDIR /app

FROM base AS plan
COPY . .
WORKDIR /app
RUN rustup show
RUN cargo chef prepare --recipe-path recipe.json

FROM base as build
COPY --from=plan /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin service-mango-fills --bin service-mango-pnl --bin service-mango-orderbook

FROM debian:bullseye-slim as run
RUN apt-get update && apt-get -y install ca-certificates libc6
COPY --from=build /app/target/release/service-mango-* /usr/local/bin/
COPY --from=build /app/service-mango-pnl/conf/template-config.toml ./pnl-config.toml
COPY --from=build /app/service-mango-fills/conf/template-config.toml ./fills-config.toml
COPY --from=build /app/service-mango-orderbook/conf/template-config.toml ./orderbook-config.toml