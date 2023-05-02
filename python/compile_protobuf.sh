PATH_TO_APTOS_CORE=${PATH_TO_APTOS_CORE:-"../../aptos-core"}
python3 -m grpc_tools.protoc --proto_path=$PATH_TO_APTOS_CORE"/crates/aptos-protos/proto/" aptos/indexer/v1/raw_data.proto aptos/util/timestamp/timestamp.proto aptos/transaction/testing1/v1/transaction.proto --python_out=. --pyi_out=. --grpc_python_out=.
