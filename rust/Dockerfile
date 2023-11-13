### Indexer GRPC Image ###

FROM rust as rust-base

WORKDIR /app

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

COPY --link . /app

# Create the release build for aptos-processor-service.
RUN ls /
RUN ls /app
RUN cargo build --locked --release -p aptos-processor-service
RUN cp target/release/aptos-processor-service /usr/local/bin

# The health check port
EXPOSE 8080

# The gRPC port
EXPOSE 50051

ENV RUST_LOG_FORMAT=json

# Add build info
ARG GIT_TAG
ENV GIT_TAG ${GIT_TAG}
ARG GIT_BRANCH
ENV GIT_BRANCH ${GIT_BRANCH}
ARG GIT_SHA
ENV GIT_SHA ${GIT_SHA}
