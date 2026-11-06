#!/bin/sh

#  Copyright 2016-2026. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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
