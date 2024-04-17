### Indexer Processor Image ###

# Stage 1: Build the binary

FROM rust:slim-bullseye as builder

WORKDIR /app

COPY --link . /app

RUN apt-get update && apt-get install -y cmake curl clang git pkg-config libssl-dev libpq-dev lld
ENV CARGO_NET_GIT_FETCH_WITH_CLI true
RUN cargo build --locked --release -p processor
RUN cp target/release/processor /usr/local/bin
RUN cargo build --locked --release -p indexer-metrics
RUN cp target/release/indexer-metrics /usr/local/bin

# add build info
ARG GIT_TAG
ENV GIT_TAG ${GIT_TAG}
ARG GIT_BRANCH
ENV GIT_BRANCH ${GIT_BRANCH}
ARG GIT_SHA
ENV GIT_SHA ${GIT_SHA}

# Stage 2: Create the final image

FROM debian:bullseye-slim

COPY --from=builder /usr/local/bin/processor /usr/local/bin
COPY --from=builder /usr/local/bin/indexer-metrics /usr/local/bin

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install --no-install-recommends -y \
        libssl1.1 \
        ca-certificates \
        net-tools \
        tcpdump \
        iproute2 \
        netcat \
        libpq-dev \
        curl

ENV RUST_LOG_FORMAT=json

# add build info
ARG GIT_TAG
ENV GIT_TAG ${GIT_TAG}
ARG GIT_BRANCH
ENV GIT_BRANCH ${GIT_BRANCH}
ARG GIT_SHA
ENV GIT_SHA ${GIT_SHA}

# The health check port
EXPOSE 8084

ENTRYPOINT ["/usr/local/bin/processor"]
