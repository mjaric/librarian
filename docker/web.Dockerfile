# librarian-web — bookshelf catalogue web image.
#
# Build:   docker compose build librarian-web
# Run:     docker compose up -d librarian-web   (with postgres + daemon)
#
# Two artifacts come out of the build stage: the server binary and the
# wasm UI bundle (xtask dist). Runtime needs nothing but glibc — no rsync,
# no outbound network: it only reads Postgres and the read-only /data mount.

FROM rust:1.97-slim-bookworm AS build
WORKDIR /src

# wasm toolchain for the UI bundle. wasm-bindgen-cli must match the
# wasm-bindgen crate pin in crates/bookshelf-ui/Cargo.toml (=0.2.127).
RUN rustup target add wasm32-unknown-unknown
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo install wasm-bindgen-cli --version 0.2.127 --locked

COPY Cargo.toml Cargo.lock ./
COPY crates crates

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p librarian-web \
    && cargo run --locked -p xtask -- dist \
    && cp target/release/librarian-web /usr/local/bin/librarian-web \
    && cp -r crates/librarian-web/static /usr/local/librarian-web-static

FROM debian:bookworm-slim
RUN groupadd -g 1000 bookshelf \
    && useradd -u 1000 -g 1000 -d /app -s /usr/sbin/nologin bookshelf \
    && mkdir -p /data /app \
    && chown -R 1000:1000 /app
COPY --from=build /usr/local/bin/librarian-web /usr/local/bin/librarian-web
COPY --from=build /usr/local/librarian-web-static /app/static
COPY docker/librarian-web.toml /etc/librarian/librarian-web.toml
USER 1000:1000
WORKDIR /app
# SIGTERM (docker stop) → graceful shutdown of in-flight responses.
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/librarian-web"]
CMD ["--config", "/etc/librarian/librarian-web.toml"]
