#!/bin/bash
# Copyright (c) Aptos
# SPDX-License-Identifier: Apache-2.0

# This script is to build and push all the docker images of all indexer client examples
# You need to execute this from the repository root as working directory
# E.g. scripts/build-and-push-images.sh
# If you want to build a specific example only:
#  scripts/build-and-push-images.sh <example>
# E.g. scripts/build-and-push-images.sh python
# Note that this uses kaniko (https://github.com/GoogleContainerTools/kaniko) instead of vanilla docker to build the images, which has good remote caching support

set -ex

TARGET_REGISTRY="us-docker.pkg.dev/aptos-registry/docker/indexer-client-examples"
# take GIT_SHA from environment variable if set, otherwise use git rev-parse HEAD
GIT_SHA="${GIT_SHA:-$(git rev-parse HEAD)}"
ALL_EXAMPLES=("python" "rust")
EXAMPLE_TO_BUILD_ARG="${1:-all}"

if [ "$EXAMPLE_TO_BUILD_ARG" == "all" ]; then
    EXAMPLES_TO_BUILD="$ALL_EXAMPLES"
else
    EXAMPLES_TO_BUILD="$EXAMPLE_TO_BUILD_ARG"
fi

if [ "$CI" == "true" ]; then
    CREDENTIAL_MOUNT="$HOME/.docker/:/kaniko/.docker/:ro"
else
    # locally we mount gcloud config credentials
    CREDENTIAL_MOUNT="$HOME/.config/gcloud:/root/.config/gcloud:ro"
fi

for example in $EXAMPLES_TO_BUILD; do
    docker run \
        --rm \
        -v $CREDENTIAL_MOUNT \
        -v $(pwd)/$example:/workspace \
        gcr.io/kaniko-project/executor:latest \
        --dockerfile /workspace/Dockerfile \
        --destination "$TARGET_REGISTRY/$example:$GIT_SHA" \
        --context dir:///workspace/ \
        --cache=true
done
