import os
import sys

# protoc's generated code cross-imports itself with bare names (e.g. `from run import top_level_pb2`), so
# `generated/` must be importable as a top-level package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "generated"))
