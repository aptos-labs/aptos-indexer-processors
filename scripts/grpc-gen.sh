#!/bin/bash
set -eu

# Check the number of arguments
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <lang> <proto_files_path>"
  echo "<lang> could be one of 'typescript', 'python', or 'rust'."
  exit 1
fi

# Assign arguments to variables
LANG="$1"
PROTO_FILES_PATH="$2"

# Set the output directory where the generated code will be saved
OUT_DIR="./generated"


ensure_protoc_installed() {
  if ! command -v protoc &> /dev/null; then
    echo "protoc is not installed. Installing protoc..."

    # Install protoc based on the package manager (apt-get or brew)
    if command -v apt-get &> /dev/null; then
        sudo apt-get update
        sudo apt-get install -y protobuf-compiler
    elif command -v yum > /dev/null; then
        sudo yum install -y protobuf-compiler
    elif command -v brew &> /dev/null; then
        brew install protobuf
    else
        echo "Error: Unable to install protoc. Please install it manually."
        exit 1
    fi

    echo "protoc installed successfully."
  fi
}

ensure_protoc_gen_ts_installed() {
  # Check if grpc_tools_node_protoc_ts is installed
  if ! npm list -g grpc_tools_node_protoc_ts | grep grpc_tools_node_protoc_ts &> /dev/null; then
      echo "grpc_tools_node_protoc_ts is not installed. Installing grpc_tools_node_protoc_ts..."
      npm install -g grpc_tools_node_protoc_ts
      echo "grpc_tools_node_protoc_ts installed successfully."
  fi
}


# Generate TypeScript code
generate_typescript_code() {
  ensure_protoc_installed
  ensure_protoc_gen_ts_installed

  JS_DIR="$OUT_DIR/typescript"
  mkdir -p $JS_DIR

  # Generate TypeScript code from protobuf files
  npm exec --yes --package grpc-tools -- grpc_tools_node_protoc \
    --grpc_out=grpc_js:$JS_DIR \
    --js_out="import_style=commonjs,binary:$JS_DIR" \
    -I $PROTO_FILES_PATH \
    $(find "$PROTO_FILES_PATH" -type f -name "*.proto")

   protoc \
    --ts_out="grpc_js:$JS_DIR" \
    --proto_path=$PROTO_FILES_PATH \
    $(find "$PROTO_FILES_PATH" -type f -name "*.proto")

  echo "TypeScript code generated successfully!"
}

ensure_grpcio_tools_installed() {
  PACKAGE_NAME="grpcio-tools"
  # Check if pipx is installed
  if python3 -c "import pkgutil; exit(0) if pkgutil.find_loader('$PACKAGE_NAME') else exit(1)"; then
    echo "$PACKAGE_NAME is already installed."
  else
      echo "$PACKAGE_NAME is not installed. Installing $PACKAGE_NAME..."
      python3 -m pip install $PACKAGE_NAME
      echo "$PACKAGE_NAME installed successfully."
  fi
}

# Generate Python code
generate_python_code() {
  PY_DIR="$OUT_DIR/python"
  mkdir -p $PY_DIR

  ensure_grpcio_tools_installed

  # Generate Python code from protobuf files
  python3 -m grpc_tools.protoc \
    --proto_path=$PROTO_FILES_PATH \
    --python_out=$PY_DIR \
    --pyi_out=$PY_DIR \
    --grpc_python_out=$PY_DIR \
    $(find "$PROTO_FILES_PATH" -type f -name "*.proto")

  echo "Python code generated successfully!"
}

# Generate Rust code
generate_rust_code() {
  RUST_DIR="$OUT_DIR/RUST"
  mkdir -p $RUST_DIR

  ensure_protoc_installed

  # Generate Rust code from protobuf files
  protoc \
    --rust_out=$RUST_DIR \
    -I $PROTO_FILES_PATH \
    $(find "$PROTO_FILES_PATH" -type f -name "*.proto")

  echo "Rust code generated successfully!"
}

# Check the "lang" argument and generate code accordingly
if [[ "$LANG" == "typescript" ]]; then
  generate_typescript_code
elif [[ "$LANG" == "python" ]]; then
  generate_python_code
elif [[ "$LANG" == "rust" ]]; then
  generate_rust_code
else
  echo "Unsupported language. Please provide 'typescript', 'python', or 'rust' as the argument."
  exit 1
fi
