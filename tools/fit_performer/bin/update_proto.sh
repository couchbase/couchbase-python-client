#!/bin/sh
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
