
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

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    List,
                    Union)

from couchbase.result import (CounterResult,
                              MultiCounterResult,
                              MultiMutationResult,
                              MutationResult)

if TYPE_CHECKING:
    from couchbase.options import (AppendMultiOptions,
                                   AppendOptions,
                                   DecrementMultiOptions,
                                   DecrementOptions,
                                   IncrementMultiOptions,
                                   IncrementOptions,
                                   PrependMultiOptions,
                                   PrependOptions)


class BinaryCollection:

    def __init__(self, collection):
        self._collection = collection

    def increment(
        self,
        key,  # type: str
        *opts,  # type: IncrementOptions
        **kwargs,  # type: Any
    ) -> CounterResult:
        """Increments the ASCII value of the document, specified by the key, by the amount indicated in the delta
        option (defaults to 1).

        Args:
            key (str): The key of the document to increment.
            opts (:class:`~couchbase.options.IncrementOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.IncrementOptions`

        Returns:
            :class:`~couchbase.result.CounterResult`: An instance of :class:`~couchbase.result.CounterResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:

            Simple increment operation::

                from couchbase.options import IncrementOptions, SignedInt64

                # ... other code ...

                collection = bucket.default_collection()
                res = collection.binary().increment('counter-doc', IncrementOptions(initial=SignedInt64(100))
                print(f'Counter value: {res.content}')

            Simple increment operation, change default delta::

                from couchbase.options import IncrementOptions, DeltaValue

                # ... other code ...

                collection = bucket.default_collection()
                res = collection.binary().increment('counter-doc', IncrementOptions(delta=DeltaValue(5))
                print(f'Counter value: {res.content}')

        """
        return self._collection._increment(key, *opts, **kwargs)

    def decrement(
        self,
        key,  # type: str
        *opts,  # type: DecrementOptions
        **kwargs,  # type: Any
    ) -> CounterResult:
        """Decrements the ASCII value of the document, specified by the key, by the amount indicated in the delta
        option (defaults to 1).

        Args:
            key (str): The key of the document to decrement.
            opts (:class:`~couchbase.options.DecrementOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.DecrementOptions`

        Returns:
            :class:`~couchbase.result.CounterResult`: An instance of :class:`~couchbase.result.CounterResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:

            Simple decrement operation::

                from couchbase.options import DecrementOptions, SignedInt64

                # ... other code ...

                collection = bucket.default_collection()
                res = collection.binary().decrement('counter-doc', DecrementOptions(initial=SignedInt64(100))
                print(f'Counter value: {res.content}')

            Simple decrement operation, change default delta::

                from couchbase.options import DecrementOptions, DeltaValue

                # ... other code ...

                collection = bucket.default_collection()
                res = collection.binary().decrement('counter-doc', DecrementOptions(delta=DeltaValue(5))
                print(f'Counter value: {res.content}')

        """
        return self._collection._decrement(key, *opts, **kwargs)

    def append(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: AppendOptions
        **kwargs,  # type: Any
    ) -> MutationResult:
        """Appends the specified value to the beginning of document of the specified key.

        Args:
            key (str): The key of the document to append to.
            value (Union[str, bytes, bytearray]): The value to append to the document.
            opts (:class:`~couchbase.options.AppendOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.AppendOptions`

        Returns:
            :class:`~couchbase.result.MutationResult`: An instance of :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:

            Simple append string operation::

                # ... other code ...

                collection = bucket.default_collection()
                res = collection.binary().append('string-doc', 'XYZ')

            Simple append binary operation::

                # ... other code ...

                collection = bucket.default_collection()
                res = collection.binary().append('binary-doc', b'XYZ')

            Simple append operation with options::

                from datetime import timedelta

                from couchbase.options import AppendOptions

                # ... other code ...

                collection = bucket.default_collection()
                res = collection.binary().append('string-doc',
                                                'XYZ',
                                                AppendOptions(timeout=timedelta(seconds=2)))

        """
        return self._collection._append(key, value, *opts, **kwargs)

    def prepend(
        self,
        key,  # type: str
        value,  # type: Union[str,bytes,bytearray]
        *opts,  # type: PrependOptions
        **kwargs,  # type: Any
    ) -> MutationResult:
        """Prepends the specified value to the beginning of document of the specified key.

        Args:
            key (str): The key of the document to prepend to.
            value (Union[str, bytes, bytearray]): The value to prepend to the document.
            opts (:class:`~couchbase.options.PrependOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.PrependOptions`

        Returns:
            :class:`~couchbase.result.MutationResult`: An instance of :class:`~couchbase.result.MutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist
                on the server.

        Examples:

            Simple prepend string operation::

                # ... other code ...

                collection = bucket.default_collection()
                res = collection.binary().prepend('string-doc', 'ABC')

            Simple prepend binary operation::

                # ... other code ...

                collection = bucket.default_collection()
                res = collection.binary().prepend('binary-doc', b'ABC')

            Simple prepend operation with options::

                from datetime import timedelta

                from couchbase.options import PrependOptions

                # ... other code ...

                collection = bucket.default_collection()
                res = collection.binary().prepend('string-doc',
                                                'ABC',
                                                PrependOptions(timeout=timedelta(seconds=2)))

        """
        return self._collection._prepend(key, value, *opts, **kwargs)

    def append_multi(
        self,
        keys_and_values,  # type: Dict[str, Union[str,bytes,bytearray]]
        *opts,  # type: AppendMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiMutationResult:
        """For each key-value pair, appends the specified value to the end of the document specified by the key.

        Args:
            keys_and_values (Dict[str, Union[str,bytes,bytearray]]): The key-value pairs to use for the multiple
                append operations.  Each key should correspond to the document to append to and each value should
                correspond to the value to append to the document.
            opts (:class:`~couchbase.options.AppendMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.AppendMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiMutationResult`: An instance of
            :class:`~couchbase.result.MultiMutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on
                the server and the return_exceptions options is False.  Otherwise the exception is returned
                as a match to the key, but is not raised.

        Examples:

            Simple append-multi string operation::

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['str-doc1', 'str-doc2', 'str-doc3']
                values = ['foo', 'bar', 'baz']
                keys_and_docs = dict(zip(keys, values))
                res = collection.binary().append_multi(keys_and_docs)

            Simple append-multi binary operation::

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['bin-doc1', 'bin-doc2', 'bin-doc3']
                values = [b'foo', b'bar', b'baz']
                keys_and_docs = dict(zip(keys, values))
                res = collection.binary().append_multi(keys_and_docs)

            Simple append-multi operation with options::

                from datetime import timedelta

                from couchbase.options import AppendMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['bin-doc1', 'bin-doc2', 'bin-doc3']
                values = [b'foo', b'bar', b'baz']
                keys_and_docs = dict(zip(keys, values))
                res = collection.binary().append_multi(keys_and_docs,
                                                    AppendMultiOptions(timeout=timedelta(seconds=2)))

            Simple append-multi operation, raise an Exception if an Exception occurs::

                from couchbase.options import AppendMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['bin-doc1', 'bin-doc2', 'bin-doc3']
                values = [b'foo', b'bar', b'baz']
                keys_and_docs = dict(zip(keys, values))
                res = collection.binary().append_multi(keys_and_docs,
                                                    AppendMultiOptions(return_exceptions=False))

            Simple append-multi operation, individual key options::

                from datetime import timedelta

                from couchbase.options import AppendMultiOptions, AppendOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['bin-doc1', 'bin-doc2', 'bin-doc3']
                values = [b'foo', b'bar', b'baz']
                keys_and_docs = dict(zip(keys, values))
                per_key_opts = {'bin-doc1': AppendOptions(timeout=timedelta(seconds=10))}
                res = collection.binary().append_multi(keys_and_docs,
                                                    AppendMultiOptions(return_exceptions=False,
                                                    per_key_options=per_key_opts))


        """
        return self._collection._append_multi(keys_and_values, *opts, **kwargs)

    def prepend_multi(
        self,
        keys_and_values,  # type: Dict[str, Union[str,bytes,bytearray]]
        *opts,  # type: PrependMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiMutationResult:
        """For each key-value pair, prepends the specified value to the beginning of the document specified
        by the key.

        Args:
            keys_and_values (Dict[str, Union[str,bytes,bytearray]]): The key-value pairs to use for the multiple
                prepend operations.  Each key should correspond to the document to prepend to and each value should
                correspond to the value to prepend to the document.
            opts (:class:`~couchbase.options.PrependMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.PrependMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiMutationResult`: An instance of
            :class:`~couchbase.result.MultiMutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a match
                to the key, but is not raised.

        Examples:

            Simple prepend-multi string operation::

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['str-doc1', 'str-doc2', 'str-doc3']
                values = ['foo', 'bar', 'baz']
                keys_and_docs = dict(zip(keys, values))
                res = collection.binary().prepend_multi(keys_and_docs)

            Simple prepend-multi binary operation::

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['bin-doc1', 'bin-doc2', 'bin-doc3']
                values = [b'foo', b'bar', b'baz']
                keys_and_docs = dict(zip(keys, values))
                res = collection.binary().prepend_multi(keys_and_docs)

            Simple prepend-multi operation with options::

                from datetime import timedelta

                from couchbase.options import PrependMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['bin-doc1', 'bin-doc2', 'bin-doc3']
                values = [b'foo', b'bar', b'baz']
                keys_and_docs = dict(zip(keys, values))
                res = collection.binary().prepend_multi(keys_and_docs,
                                                    PrependMultiOptions(timeout=timedelta(seconds=2)))

            Simple prepend-multi operation, raise an Exception if an Exception occurs::

                from couchbase.options import PrependMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['bin-doc1', 'bin-doc2', 'bin-doc3']
                values = [b'foo', b'bar', b'baz']
                keys_and_docs = dict(zip(keys, values))
                res = collection.binary().prepend_multi(keys_and_docs,
                                                    PrependMultiOptions(return_exceptions=False))

            Simple prepend-multi operation, individual key options::

                from datetime import timedelta

                from couchbase.options import PrependMultiOptions, PrependOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['bin-doc1', 'bin-doc2', 'bin-doc3']
                values = [b'foo', b'bar', b'baz']
                keys_and_docs = dict(zip(keys, values))
                per_key_opts = {'bin-doc1': PrependOptions(timeout=timedelta(seconds=10))}
                res = collection.binary().prepend_multi(keys_and_docs,
                                                    PrependMultiOptions(return_exceptions=False,
                                                    per_key_options=per_key_opts))


        """
        return self._collection._prepend_multi(keys_and_values, *opts, **kwargs)

    def increment_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: IncrementMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiCounterResult:
        """For each key in the provided list, increments the ASCII value of the document, specified by the key,
        by the amount indicated in the delta option (defaults to 1).

        Args:
            keys (List[str]): The keys to use for the multiple increment operations.  Each key should correspond
                to the document to increment.
            opts (:class:`~couchbase.options.IncrementMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.IncrementMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiMutationResult`: An instance of
            :class:`~couchbase.result.MultiMutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on
                the server and the return_exceptions options is False.  Otherwise the exception is returned
                as a match to the key, but is not raised.

        Examples:

            Simple increment-multi operation, set initial value::

                from couchbase.options import IncrementMultiOptions, SignedInt64

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['counter_doc1', 'counter_doc1', 'counter_doc1']
                res = collection.binary().increment_multi(keys, IncrementMultiOptions(initial=SignedInt64(100))
                for k, v in res.results.items():
                    print(f'Counter doc {k} has value: {v.content}')

            Simple increment-multi operation, change default delta::

                from couchbase.options import IncrementMultiOptions, DeltaValue

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['counter_doc1', 'counter_doc1', 'counter_doc1']
                res = collection.binary().increment_multi(keys, IncrementMultiOptions(delta=DeltaValue(5))
                for k, v in res.results.items():
                    print(f'Counter doc {k} has value: {v.content}')


            Simple increment-multi operation, raise an Exception if an Exception occurs::

                from couchbase.options import IncrementMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['counter_doc1', 'counter_doc1', 'counter_doc1']
                res = collection.binary().increment_multi(keys,
                                                    IncrementMultiOptions(return_exceptions=False))
                for k, v in res.results.items():
                    print(f'Counter doc {k} has value: {v.content}')

            Simple increment-multi operation, individual key options::

                from datetime import timedelta

                from couchbase.options import IncrementMultiOptions, IncrementOptions, DeltaValue, SignedInt64

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['counter_doc1', 'counter_doc1', 'counter_doc1']
                per_key_opts = {'counter_doc1': IncrementOptions(delta=DeltaValue(100),initial=SignedInt64(100))}
                res = collection.binary().increment_multi(keys,
                                                    IncrementMultiOptions(per_key_options=per_key_opts))
                for k, v in res.results.items():
                    print(f'Counter doc {k} has value: {v.content}')


        """
        return self._collection._increment_multi(keys, *opts, **kwargs)

    def decrement_multi(
        self,
        keys,  # type: List[str]
        *opts,  # type: DecrementMultiOptions
        **kwargs,  # type: Dict[str, Any]
    ) -> MultiCounterResult:
        """For each key in the provided list, decrements the ASCII value of the document, specified by the key,
        by the amount indicated in the delta option (defaults to 1).

        Args:
            keys (List[str]): The keys to use for the multiple decrement operations.  Each key should correspond
                to the document to decrement.
            opts (:class:`~couchbase.options.DecrementMultiOptions`): Optional parameters for this operation.
            **kwargs (Dict[str, Any]): keyword arguments that can be used in place or to
                override provided :class:`~couchbase.options.DecrementMultiOptions`

        Returns:
            :class:`~couchbase.result.MultiMutationResult`: An instance of
            :class:`~couchbase.result.MultiMutationResult`.

        Raises:
            :class:`~couchbase.exceptions.DocumentNotFoundException`: If the key provided does not exist on the
                server and the return_exceptions options is False.  Otherwise the exception is returned as a
                match to the key, but is not raised.

        Examples:

            Simple decrement-multi operation, set initial value::

                from couchbase.options import DecrementMultiOptions, SignedInt64

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['counter_doc1', 'counter_doc1', 'counter_doc1']
                res = collection.binary().decrement_multi(keys, DecrementMultiOptions(initial=SignedInt64(100))
                for k, v in res.results.items():
                    print(f'Counter doc {k} has value: {v.content}')

            Simple decrement-multi operation, change default delta::

                from couchbase.options import DecrementMultiOptions, DeltaValue

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['counter_doc1', 'counter_doc1', 'counter_doc1']
                res = collection.binary().decrement_multi(keys, DecrementMultiOptions(delta=DeltaValue(5))
                for k, v in res.results.items():
                    print(f'Counter doc {k} has value: {v.content}')


            Simple decrement-multi operation, raise an Exception if an Exception occurs::

                from couchbase.options import DecrementMultiOptions

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['counter_doc1', 'counter_doc1', 'counter_doc1']
                res = collection.binary().decrement_multi(keys,
                                                    DecrementMultiOptions(return_exceptions=False))
                for k, v in res.results.items():
                    print(f'Counter doc {k} has value: {v.content}')

            Simple decrement-multi operation, individual key options::

                from datetime import timedelta

                from couchbase.options import DecrementMultiOptions, DecrementOptions, DeltaValue, SignedInt64

                # ... other code ...

                collection = bucket.default_collection()
                keys = ['counter_doc1', 'counter_doc1', 'counter_doc1']
                per_key_opts = {'counter_doc1': DecrementOptions(delta=DeltaValue(100),initial=SignedInt64(100))}
                res = collection.binary().decrement_multi(keys,
                                                    DecrementMultiOptions(per_key_options=per_key_opts))
                for k, v in res.results.items():
                    print(f'Counter doc {k} has value: {v.content}')


        """
        return self._collection._decrement_multi(keys, *opts, **kwargs)
