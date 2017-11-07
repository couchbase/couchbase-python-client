#
# Copyright 2017, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from couchbase.tests.base import ConnectionTestCase
import jsonschema

# For Python 2/3 compatibility
try:
    basestring
except NameError:
    basestring = str


class HealthTest(ConnectionTestCase):
    def setUp(self):
        super(HealthTest, self).setUp()
        self.skipUnlessMock()

    server_schema = {"type": "object",
                     "properties": {"details": {"type": "string"},
                                    "latency": {"anyOf": [{"type": "number"}, {"type": "string"}]},
                                    "server": {"type": "string"},
                                    "status": {"type": "number"}
                                    },
                     "required": ["details", "latency", "server", "status"]}

    servers_schema = {"type": "array",
                      "items": server_schema}

    @staticmethod
    def gen_schema(name):
        return {"type": "object",
                "properties": {name: HealthTest.servers_schema},
                "required": [name]
                }

    def test_health(self):
        result = self.cb.get_health()

        services_schema = {"anyOf":
                               [HealthTest.gen_schema(name) for name in ["n1ql", "views", "fts", "kv"]]
                           }

        health_schema = {"anyOf": [{
            "type": "object",
            "properties": {
                "services": services_schema
            },
            "required": ["services"]
        }]}

        jsonschema.validate(result, services_schema)


if __name__ == '__main__':
    unittest.main()
