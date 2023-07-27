#!/bin/bash
# This script is to generate the protobuf files; currently there is no easy way for python code
# without using remote registry or compiling github.com/grpc/grpc from source.
#
# The next step is to replace python generation from python cli to `grpc_python``.
for file in *.gen.yaml
do
    # Skip Python for now.
    if [[ $file == *"python"* ]]; then
        continue
    fi
    buf generate --template "$file"
done

python3 -m grpc_tools.protoc --proto_path=./proto --python_out=python --pyi_out=python --grpc_python_out=python \
    proto/aptos/bigquery_schema/v1/transaction.proto \
    proto/aptos/indexer/v1/raw_data.proto \
    proto/aptos/internal/fullnode/v1/fullnode_data.proto \
    proto/aptos/transaction/v1/transaction.proto \
    proto/aptos/util/timestamp/timestamp.proto
