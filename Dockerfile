# librarian — bookshelf backend daemon image.
#
# Build:   docker compose build
# Run:     docker compose up -d   (postgres + librarian)
#
# Runtime requirements baked in: rsync (the transport), CA certificates
# (HTTPS feed/repair). Runs as UID/GID 1000 so the bind-mounted library
# (~/.bookshelf on the host) stays owned by the host user.

FROM rust:1.97-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
# Cache mounts keep rebuilds incremental; the binary is copied out of the
# cache-mounted target dir within the same RUN.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p librarian \
    && cp target/release/librarian /usr/local/bin/librarian

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends rsync ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 1000 bookshelf \
    && useradd -u 1000 -g 1000 -d /data -s /usr/sbin/nologin bookshelf \
    && mkdir -p /data /var/lib/librarian \
    && chown -R 1000:1000 /data /var/lib/librarian
COPY --from=build /usr/local/bin/librarian /usr/local/bin/librarian
COPY docker/librarian.toml /etc/librarian/librarian.toml
USER 1000:1000
WORKDIR /data
# SIGTERM (docker stop) → graceful shutdown: running job requeued, rsync
# children group-killed by the kernel; exits 0.
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/librarian"]
CMD ["daemon", "--config", "/etc/librarian/librarian.toml"]
