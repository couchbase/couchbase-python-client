#!/bin/sh
# generate.sh [out_dir]
#
# Generates the Python protobuf/gRPC code from proto/ into out_dir, generated/ by default.

set -e

cd "$(dirname "$0")/.."

out=${1:-generated}
rm -rf "$out"
mkdir -p "$out"
python -m grpc_tools.protoc \
    --proto_path=./proto \
    --python_out="$out" \
    --grpc_python_out="$out" \
    --mypy_out=quiet:"$out" \
    --mypy_grpc_out=quiet:"$out" \
    ./proto/*.proto
