# --- Build Stage ---
FROM rust:1.85-slim-bookworm as builder

# Install system dependencies
RUN apt-get update && apt-get install -y 
    pkg-config 
    libssl-dev 
    protobuf-compiler 
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the source code
COPY . .

# Build the application
RUN cargo build --release

# --- Runtime Stage ---
FROM debian:bookworm-slim

# Install runtime dependencies (OpenSSL is often needed)
RUN apt-get update && apt-get install -y 
    libssl3 
    ca-certificates 
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/target/release/investengine_csv_server_rs /app/

# Copy templates
COPY --from=builder /app/templates /app/templates

# Set the port from .env (default to 8000)
ARG CSV_SERVER_PORT=8000
ENV CSV_SERVER_PORT=${CSV_SERVER_PORT}

# Expose the configured port
EXPOSE ${CSV_SERVER_PORT}

# Run the application
CMD ["./investengine_csv_server_rs"]
