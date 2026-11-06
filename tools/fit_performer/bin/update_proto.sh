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

# update_proto.sh
#
# Refreshes proto/ from couchbaselabs/fit-protocol, then regenerates generated/.

set -e

cd "$(dirname "$0")/.."

rm -rf proto
mkdir -p proto
curl --location --fail https://github.com/couchbaselabs/fit-protocol/archive/refs/heads/main.tar.gz \
		| tar -xz --strip-components=2 -C proto fit-protocol-main/operational

./bin/generate.sh
