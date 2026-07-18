# Build from mtrxAI-org root (sibling of mtrxAI-peer/ and mtrxAI-common/):
#   docker build -f mtrxAI-peer/Dockerfile -t mtrxai/mtrx-peer .
FROM rust:1.88 AS builder

WORKDIR /usr/src/mtrxai

COPY mtrxAI-common/mtrxai-attestation ./mtrxAI-common/mtrxai-attestation
COPY mtrxAI-peer/Cargo.toml mtrxAI-peer/Cargo.lock ./mtrxAI-peer/
COPY mtrxAI-peer/peer ./mtrxAI-peer/peer
COPY mtrxAI-peer/peer-tests ./mtrxAI-peer/peer-tests
COPY mtrxAI-peer/desktop/src-tauri ./mtrxAI-peer/desktop/src-tauri

ARG MTRXAI_BUILD_ID
ARG MTRXAI_ATTESTATION_SECRET
ARG MTRXAI_VERSION=unknown
ENV MTRXAI_BUILD_ID=${MTRXAI_BUILD_ID}
ENV MTRXAI_ATTESTATION_SECRET=${MTRXAI_ATTESTATION_SECRET}
ENV MTRXAI_ATTESTATION_SKIP=1

WORKDIR /usr/src/mtrxai/mtrxAI-peer
RUN cargo build --release -p peer

# CUDA base includes nvidia-smi when the container is started with GPU access.
FROM nvidia/cuda:12.4.1-base-ubuntu22.04

ARG MTRXAI_VERSION=unknown
LABEL org.opencontainers.image.title="mtrx-peer" \
      org.opencontainers.image.version="${MTRXAI_VERSION}"

RUN apt-get update && apt-get install -y ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/bin

# Docker port publishing forwards to the container network interface, not loopback.
ENV MTRXAI_PROXY_BIND=0.0.0.0

COPY --from=builder /usr/src/mtrxai/mtrxAI-peer/target/release/peer .

ENTRYPOINT ["/usr/local/bin/peer"]
