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
from abc import ABC, abstractmethod
from typing import Any


class CouchbaseSpan(ABC):
    """
    This is an abstract base class intended to wrap an external span implementation, to use withing the
    python client.  You will want to create a class deriving from this, which implements the :meth:`set_attribute`
    and :meth:`finish` methods.
    """

    def __init__(self,
                 span   # type: Any
                 ):
        # type: (...) -> CouchbaseSpan
        """
        Construct a new :class:`~CouchbaseSpan`.
        :param: Any span: The concrete span this class will wrap.
        """
        self._span = span
        super().__init__()

    @property
    def span(self):
        # type: (...) -> Any
        """
        Return the underlying wrapped span object.
        :return: The underlying span object this class wrapped.
        """
        return self._span

    @abstractmethod
    def set_attribute(self,
                      key,      # type: str
                      value     # type: Any
                      ):
        # type: (...) -> None
        """
        This method will need to be implemented in derived classes.  Given a key, and a value, use the underlying
        `self._span` to set an attribute on the span.

        :param: str key: Key for the attribute.
        :param: Any value: Value of the attribute.
        """
        pass

    @abstractmethod
    def finish(self):
        # type: (...) -> None
        """
        This method will need to be implemented in derived classes.  Using `self._span`, the intent is to finish the
        span.
        """
        pass


class CouchbaseTracer(ABC):
    """
    This is an abstract base class, intended to wrap a concrete tracer implementation.  There is a single method,
    :meth:`start_span`, which must be implemented in the derived class.
    """

    def __init__(self,
                 external_tracer    # type: Any
                 ):
        # type: (...) -> CouchbaseTracer
        """
        Construct a new :class:`~.CouchbaseTracer`.
        :param: Any external_tracer:  The concrete tracer which this class will wrap.
        """
        self._external_tracer = external_tracer
        super().__init__()

    @abstractmethod
    def start_span(self,
                   name,    # type: str
                   parent=None   # type: CouchbaseSpan
                   ):
        # type: (...) -> CouchbaseSpan
        """
        This method must be implemented in derived classes.  The intent is to use the underlying `self._tracer` to
        actually start a span, and return it wrapped in a :class:`CouchbaseSpan`.
        for and example.

        :param: str name: Name of the span.
        :param CouchbaseSpan parent: Parent span, if any.  Will
               be None when this is to be a top-level span.
        :return: A new CouchbaseSpan, wrapping a span created by the
        wrapped tracer.
        """
        pass
