#!/bin/sh
# generate.sh
#
# Generates the Python protobuf/gRPC code from proto/ into generated/.

set -e

cd "$(dirname "$0")/.."

rm -rf generated
mkdir -p generated
python -m grpc_tools.protoc \
    --proto_path=./proto \
    --python_out=./generated \
    --grpc_python_out=./generated \
    --mypy_out=./generated \
    ./proto/*.proto
