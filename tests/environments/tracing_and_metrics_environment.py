#  Copyright 2016-2023. Couchbase, Inc.
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


from __future__ import annotations

from typing import (Any,
                    Dict,
                    List)

from couchbase.metrics import CouchbaseMeter, CouchbaseValueRecorder
from couchbase.tracing import CouchbaseSpan, CouchbaseTracer
from tests.environments.test_environment import TestEnvironment


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


class TestSpan(CouchbaseSpan):
    def __init__(self, name):
        super().__init__(None)
        self.finished_ = False
        self.name_ = name
        self.attributes_ = dict()
        self.parent_ = None
        self._span = None

    def set_attribute(self, key, value):
        self.attributes_[key] = value

    def set_parent(self, parent):
        self.parent_ = parent

    def get_parent(self):
        return self.parent_

    def finish(self):
        self.finished_ = True

    def is_finished(self):
        return self.finished_

    def get_attributes(self):
        return self.attributes_

    def get_name(self):
        return self.name_


class TestTracer(CouchbaseTracer):
    def __init__(self):
        self.spans_ = list()

    def start_span(self, name, parent=None, **kwargs):
        span = TestSpan(name)
        span.set_parent(parent)
        self.spans_.append(span)
        return span

    def reset(self):
        self.spans_ = list()

    def spans(self):
        return self.spans_


class TracingAndMetricsTestEnvironment(TestEnvironment):

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):
        self._tracer = kwargs.pop('tracer', None)
        self._meter = kwargs.pop('meter', None)
        super().__init__(**kwargs)

    @property
    def meter(self):
        return self._meter

    @property
    def tracer(self):
        return self._tracer

    def validate_metrics(self, op):
        # default recorder is NOOP
        keys = list(self.meter.recorders().keys())
        values = list(self.meter.recorders().values())
        assert len(self.meter.recorders()) == 2
        assert op in keys[1]
        assert isinstance(values[1], BasicValueRecorder)
        assert len(values[1].values) == 1
        assert isinstance(values[1].values[0], int)

    @classmethod
    def from_environment(cls,
                         env,  # type: TestEnvironment
                         **kwargs,  # type: Dict[str, Any]
                         ) -> TracingAndMetricsTestEnvironment:

        base_env_args = {
            'couchbase_config': env.config,
            'data_provider': env.data_provider,
        }

        meter = None
        if 'create_meter' in kwargs:
            meter = BasicMeter()
            base_env_args['meter'] = meter

        tracer = None
        if 'create_tracer' in kwargs:
            tracer = TestTracer()
            base_env_args['tracer'] = tracer

        # we have to create a new environment b/c we need a new cluster in order to set the tracer
        cb_env = TestEnvironment.get_environment(**base_env_args)
        env_args = {
            'bucket': cb_env.bucket,
            'cluster': cb_env.cluster,
            'default_collection': cb_env.default_collection,
            'couchbase_config': cb_env.config,
            'data_provider': cb_env.data_provider,
        }

        if meter:
            env_args['meter'] = meter

        if tracer:
            env_args['tracer'] = tracer

        return cls(**env_args)
