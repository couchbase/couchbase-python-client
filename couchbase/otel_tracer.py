# Copyright 2021, Couchbase, Inc.
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

from typing import Any

from opentelemetry.trace import (Span,
                                 Tracer,
                                 set_span_in_context)

from couchbase.tracing import CouchbaseSpan, CouchbaseTracer


class CouchbaseOtelSpan(CouchbaseSpan):
    def __init__(self,
                 otel_span      # type: Span
                 ):
        super().__init__(otel_span)

    def set_attribute(self,
                      key,      # type: str
                      value     # type: Any
                      ):
        # type: (...) -> None
        self.span.set_attribute(key=key, value=value)

    def finish(self):
        # type: (...) -> None
        self.span.end()


class CouchbaseOtelTracer(CouchbaseTracer):
    def __init__(self,
                 otel_tracer    # type: Tracer
                 ):
        # type: (...) -> CouchbaseOtelTracer
        super().__init__(otel_tracer)

    def start_span(self,
                   name,        # type: str
                   parent=None       # type: CouchbaseOtelSpan
                   ):
        # type: (...) -> CouchbaseOtelSpan
        kwargs = {}
        if parent:
            kwargs['context'] = set_span_in_context(parent.span)
        return CouchbaseOtelSpan(
            self._external_tracer.start_span(name, **kwargs))

    def __deepcopy__(self, memo):
        """
        This prevents deepcopies, as the underlying opentelemetry tracer doesn't support a deepcopy.
        :param memo: The object that we are copying into.
        :return: The copy - None in this case
        """
        memo[id(self)] = None
        return None
