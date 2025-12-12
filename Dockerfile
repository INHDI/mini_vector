###### Builder stage ###########################################################
FROM rust:1.91 as builder
WORKDIR /app

# Build deps for openssl if needed by reqwest
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release

###### Runtime stage (distroless) #############################################
FROM gcr.io/distroless/cc-debian12
WORKDIR /app

COPY --from=builder /app/target/release/mini_vector /usr/local/bin/mini_vector
# Default config path; override with -v /path:/app/mini_vector.yml or custom arg
COPY mini_vector.yml /app/mini_vector.yml

ENTRYPOINT ["/usr/local/bin/mini_vector"]
CMD ["/app/mini_vector.yml"]

