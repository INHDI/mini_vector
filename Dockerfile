# Builder stage
FROM rust:1.91 as builder
WORKDIR /app

# Install dependencies if needed (e.g. for openssl-sys)
# notify might need some system libs depending on features, but mostly standard.
# reqwest needs openssl usually.
RUN apt-get update && apt-get install -y pkg-config libssl-dev

COPY . .
RUN cargo build --release

# Runner stage
FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/mini_vector /usr/local/bin/mini_vector
COPY mini_vector.yml /app/mini_vector.yml

CMD ["mini_vector", "/app/mini_vector.yml"]

