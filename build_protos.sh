#!/bin/bash

# Change to current directory.
cd "$(dirname "$0")"

# Generate code for Rust and TS.
for file in *.gen.yaml
do
    # For Python we use the Python toolchain.
    if [[ $file == *"python"* ]]; then
        continue
    fi
    buf generate --template "$file"
done

# Generate code for Python. Currently there is no easy way to use buf for Python
# without using a remote registry or compiling github.com/grpc/grpc from source,
# so instead we use the Python toolchain, specifically grpc_tools.protoc.
cd python/aptos-indexer-protos
poetry install
poetry run poe generate
