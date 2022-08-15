#  Copyright 2016-2022. Couchbase, Inc.
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

from typing import Dict, List

import pytest

from couchbase.exceptions import CouchbaseException, DocumentNotFoundException
from couchbase.metrics import CouchbaseMeter, CouchbaseValueRecorder

from ._test_utils import KVPair, TestEnvironment


class BasicValueRecorder(CouchbaseValueRecorder):
    def __init__(self):
        self._values = []
        super().__init__()

    @property
    def values(self) -> List[int]:
        return self._values

    def record_value(self, value: int) -> None:
        self._values.append(value)


class NoOpRecorder(CouchbaseValueRecorder):
    def __init__(self):
        super().__init__()

    def record_value(self, value: int) -> None:
        pass


class BasicMeter(CouchbaseMeter):
    _CB_OPERATION = 'db.couchbase.operations'
    _CB_SERVICE = 'db.couchbase.service'
    _CB_OP = 'db.operation'

    def __init__(self):
        self._recorders = {'NOOP': NoOpRecorder()}
        super().__init__()

    def value_recorder(self,
                       name,      # type: str
                       tags       # type: Dict[str, str]
                       ) -> CouchbaseValueRecorder:

        if name != self._CB_OPERATION:
            return self._recorders['NOOP']

        svc = tags.get(self._CB_SERVICE, None)
        if not svc:
            return self._recorders['NOOP']

        op_type = tags.get(self._CB_OP, None)
        if not op_type:
            return self._recorders['NOOP']

        key = f'{svc}::{op_type}'
        recorder = self._recorders.get(key, None)
        if recorder:
            return recorder

        recorder = BasicValueRecorder()
        self._recorders[key] = recorder
        return recorder

    def recorders(self) -> Dict[str, CouchbaseValueRecorder]:
        return self._recorders

    def reset(self) -> None:
        self._recorders = {'NOOP': NoOpRecorder()}


class MetricsTests:
    METER = BasicMeter()

    @pytest.fixture(scope="class", name="cb_env")
    def couchbase_test_environment(self, couchbase_config):

        cb_env = TestEnvironment.get_environment(__name__,
                                                 couchbase_config,
                                                 manage_buckets=True,
                                                 meter=self.METER)
        cb_env.try_n_times(3, 5, cb_env.load_data)

        yield cb_env
        cb_env.try_n_times(3, 5, cb_env.purge_data)

    @pytest.fixture(name="default_kvp")
    def default_key_and_value(self, cb_env) -> KVPair:
        key, value = cb_env.get_default_key_value()
        yield KVPair(key, value)

    @pytest.fixture(name="new_kvp")
    def new_key_and_value_with_reset(self, cb_env) -> KVPair:
        key, value = cb_env.get_new_key_value()
        yield KVPair(key, value)
        cb_env.try_n_times_till_exception(10,
                                          1,
                                          cb_env.collection.remove,
                                          key,
                                          expected_exceptions=(DocumentNotFoundException,),
                                          reset_on_timeout=True,
                                          reset_num_times=3)

    @pytest.fixture()
    def skip_if_mock(self, cb_env):
        if cb_env.is_mock_server:
            pytest.skip("Test needs real server")

    # @TODO(jc): CXXCBC-207
    # @pytest.fixture()
    # def setup_query(self, cb_env):
    #     cb_env.check_if_feature_supported('query_index_mgmt')
    #     ixm = cb_env.cluster.query_indexes()
    #     cb_env.try_n_times(10, 3, ixm.create_primary_index,
    #                 cb_env.bucket.name,
    #                 timeout=timedelta(seconds=60),
    #                 ignore_if_exists=True)
    #     yield
    #     cb_env.try_n_times_till_exception(10, 3,
    #                                 ixm.drop_primary_index,
    #                                 cb_env.bucket.name,
    #                                 expected_exceptions=(QueryIndexNotFoundException))

    def _validate_metrics(self, op):
        # default recorder is NOOP
        keys = list(self.METER.recorders().keys())
        values = list(self.METER.recorders().values())
        assert len(self.METER.recorders()) == 2
        assert op in keys[1]
        assert isinstance(values[1], BasicValueRecorder)
        assert len(values[1].values) == 1
        assert isinstance(values[1].values[0], int)

    @pytest.mark.parametrize("op", ["get", "upsert", "insert", "replace", "remove"])
    def test_custom_logging_meter_kv(self, cb_env, default_kvp, new_kvp, op):
        self.METER.reset()
        cb = cb_env.collection
        operation = getattr(cb, op)
        try:
            if op == 'insert':
                operation(new_kvp.key, new_kvp.value)
            elif op in ['get', 'remove']:
                operation(default_kvp.key)
            else:
                operation(default_kvp.key, default_kvp.value)
        except CouchbaseException:
            pass

        self._validate_metrics(op)

    # @TODO(jc): CXXCBC-207
    # @pytest.mark.usefixtures("skip_if_mock")
    # @pytest.mark.usefixtures("setup_query")
    # def test_custom_logging_meter_query(self, cb_env):
    #     self.METER.reset()
    #     result = cb_env.cluster.query(f"SELECT * FROM `{cb_env.bucket.name}` LIMIT 2").execute()
    #     self._validate_metrics('n1ql')

    # @TODO(jc): CXXCBC-207
    # @pytest.mark.usefixtures("skip_if_mock")
    # def test_custom_logging_meter_analytics(self, cb_env):
    #     self.METER.reset()

    # @TODO(jc): CXXCBC-207
    # @pytest.mark.usefixtures("skip_if_mock")
    # def test_custom_logging_meter_search(self, cb_env, default_kvp, new_kvp, op):
    #     self.METER.reset()

    # @TODO(jc): CXXCBC-207
    # @pytest.mark.usefixtures("skip_if_mock")
    # def test_custom_logging_meter_views(self, cb_env, default_kvp, new_kvp, op):
    #     self.METER.reset()
