# Build from mtrxAI-org root (sibling of peer/ and common/):
#   docker build -f peer/Dockerfile -t mtrxai-client .
FROM rust:1.88 AS builder

WORKDIR /usr/src/mtrxai

COPY common/mtrxai-attestation ./common/mtrxai-attestation
COPY peer/Cargo.toml peer/Cargo.lock ./peer/
COPY peer/peer ./peer/peer
COPY peer/peer-tests ./peer/peer-tests
COPY peer/desktop/src-tauri ./peer/desktop/src-tauri

ARG MTRXAI_BUILD_ID
ARG MTRXAI_ATTESTATION_SECRET
ARG MTRXAI_VERSION=unknown
ENV MTRXAI_BUILD_ID=${MTRXAI_BUILD_ID}
ENV MTRXAI_ATTESTATION_SECRET=${MTRXAI_ATTESTATION_SECRET}
ENV MTRXAI_ATTESTATION_SKIP=1

WORKDIR /usr/src/mtrxai/peer
RUN cargo build --release -p peer

# CUDA base includes nvidia-smi when the container is started with GPU access.
FROM nvidia/cuda:12.4.1-base-ubuntu22.04

ARG MTRXAI_VERSION=unknown
LABEL org.opencontainers.image.title="mtrxai-client" \
      org.opencontainers.image.version="${MTRXAI_VERSION}"

RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/bin

# Docker port publishing forwards to the container network interface, not loopback.
ENV MTRXAI_PROXY_BIND=0.0.0.0

COPY --from=builder /usr/src/mtrxai/peer/target/release/peer .

ENTRYPOINT ["/usr/local/bin/peer"]
