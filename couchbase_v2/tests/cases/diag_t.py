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
import logging
from unittest import SkipTest

from couchbase_tests.base import ConnectionTestCase, PYCBC_CB_VERSION
import jsonschema
import re
import couchbase_core._libcouchbase as LCB

# For Python 2/3 compatibility
try:
    basestring
except NameError:
    basestring = str

service_schema = {"type": "object",
             "properties": {"details": {"type": "string"},
                            "latency": {"anyOf": [{"type": "number"}, {"type": "string"}]},
                            "server": {"type": "string"},
                            "status": {"type": "number"}
                            },
             "required": ["latency", "server", "status"]}

any_of_required_services_schema = {"type": "array",
                  "items": service_schema}


def gen_schema_for_services_with_required_entry(name):
    return {"type": "object",
            "properties": {name: any_of_required_services_schema},
            "required": [name]
            }


any_of_required_services_schema = {"anyOf":
                                       [gen_schema_for_services_with_required_entry(name) for name in ["n1ql", "views", "fts", "kv"]]
                                   }

ping_schema = {"anyOf": [{
    "type": "object",
    "properties": {
        "services": any_of_required_services_schema
    },
    "required": ["services"]
}]}

server_and_port_schema = {"type": "string",
                          "pattern": "([0-9]{1,3}\.){3,3}[0-9]{1,3}:[0-9]+"}
connection_status_schema = {"type": "string",
                            "pattern": "connected"}
config_schema = {"type": "array",
                 "items": {"type": "object",
                 "properties": {
                     "id": {"type": "string"},
                     "last_activity_us": {"type": "number"},
                     "local": server_and_port_schema,
                     "remote": server_and_port_schema,
                     "status": connection_status_schema
                 }}}

python_id="PYCBC"

client_id_schema = {"type": "string",
                    "pattern": "^0x[a-f0-9]+/"+python_id}

two_part_ver_num = "([0-9]+\.)+[0-9]+"

sdk_schema = {"type": "string",
              "pattern": "libcouchbase" +
                         re.escape("/") + re.escape(LCB.lcb_version()[0]) + "\s*"+
                         re.escape(PYCBC_CB_VERSION)}


diagnostics_schema = {"type": "object",
                      "properties": {
                          "config": config_schema,
                          "id": client_id_schema,
                          "sdk": sdk_schema,
                          "version": {"type": "number"}

                      }}


class DiagnosticsTests(ConnectionTestCase):

    def setUp(self):
        super(DiagnosticsTests, self).setUp()

    def test_ping(self):
        result = self.cb.ping()
        jsonschema.validate(result, any_of_required_services_schema)

    def test_diagnostics(self):
        if getattr(self.cluster_info,"network","") == "external":
            raise SkipTest("Issue with diagnostics on external network")
        if self.is_mock:
            raise SkipTest()
        result = self.cb.diagnostics()
        jsonschema.validate(result, diagnostics_schema)


if __name__ == '__main__':
    unittest.main()
