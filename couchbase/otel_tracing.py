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
from couchbase.tracing import CouchbaseTracer, CouchbaseSpan
from opentelemetry.trace import set_span_in_context, Span, Tracer


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

    """
    This wraps an OpenTelemetry Tracer, allowing the sdk to use the OpenTelemetry tracer throughout::

        from couchbase.otel_tracing import CouchbaseOtelTracer
        from opentelemetry import trace
        from opentelemetry.exporter.zipkin.json import ZipkinExporter
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.trace import set_span_in_context


        # get a tracer from opentelemetry
        trace.set_tracer_provider(TracerProvider(resource=Resource({SERVICE_NAME: "TracerZipkinTest"})))
        tracer = trace.get_tracer(__name__)

        # create a ZipkinExporter
        zipkin_exporter = ZipkinExporter()

        # Create a BatchSpanProcessor and add the exporter to it
        span_processor = BatchSpanProcessor(zipkin_exporter)

        # add to the tracer provider
        trace.get_tracer_provider().add_span_processor(span_processor)

        # wrap the tracer in a CouchbaseOtelTracer
        thetracer = CouchbaseOtelTracer(tracer)

        # use it when creating cluster.   Now, all the calls made using this cluster, or any
        # Bucket, Scope, or Collection gotten from it, will use the opentelemetry tracer, and
        # output to zipkin.
        c = Cluster("couchbase://localhost",
                    ClusterOptions(PasswordAuthenticator("Administrator", "password"), tracer=thetracer))

    """
    def __init__(self,
                 otel_tracer    # type: Tracer
                 ):
        # type: (...) -> CouchbaseOtelTracer
        super().__init__(otel_tracer)

    def start_span(self,
                   name,        # type: str
                   parent       # type: CouchbaseOtelSpan
                   ):
        # type: (...) -> CouchbaseOtelSpan
        context = set_span_in_context(parent.span) if parent else None
        return CouchbaseOtelSpan(self._external_tracer.start_span(name, context=context))

    def __deepcopy__(self, memo):
        """
        This prevents deepcopies, as the underlying opentelemetry tracer doesn't support a deepcopy.
        :param memo: The object that we are copying into.
        :return: The copy - None in this case
        """
        memo[id(self)] = None
        return None