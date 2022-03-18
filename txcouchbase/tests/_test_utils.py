
from typing import (Any,
                    Callable,
                    Tuple,
                    Type,
                    Union)

from twisted.internet import defer, reactor

from couchbase.exceptions import CouchbaseException, DocumentNotFoundException
from couchbase.transcoder import RawBinaryTranscoder, RawStringTranscoder
from tests.helpers import CollectionType  # noqa: F401
from tests.helpers import KVPair  # noqa: F401
from tests.helpers import CouchbaseTestEnvironment, CouchbaseTestEnvironmentException

from .conftest import run_in_reactor_thread


class TestEnvironment(CouchbaseTestEnvironment):
    def __init__(self, cluster, bucket, collection, cluster_config):
        super().__init__(cluster, bucket, collection, cluster_config)
        self._retry_count = 0
        self._retry_loop = None
        self._retry_result = None

    def get_new_key_value(self, reset=True):
        if reset is True:
            try:
                run_in_reactor_thread(self.collection.remove, self.NEW_KEY)
            except BaseException:
                pass
        return self.NEW_KEY, self.NEW_CONTENT

    def load_data(self):
        data_types, sample_json = self.load_data_from_file()
        for dt in data_types:
            data = sample_json.get(dt, None)
            if data and "results" in data:
                # single path
                for r in data["results"]:
                    key = f"{r['type']}_{r['id']}"
                    run_in_reactor_thread(self.collection.upsert, key, r)

                    self._loaded_keys.append(key)

                # list comprehension path, note the result will be a tuple
                # dl = wait_for_deferred1(defer.DeferredList([self.collection.upsert(
                #     f"{r['type']}_{r['id']}", r) for r in data["results"]]))
                # self._loaded_keys.extend([d[1].key for d in dl if d[0]])

    @defer.inlineCallbacks
    def load_data1(self):
        data_types, sample_json = self.load_data_from_file()
        for dt in data_types:
            data = sample_json.get(dt, None)
            if data and "results" in data:
                dl = yield defer.DeferredList([self.collection.upsert(f"{r['type']}_{r['id']}", r)
                                               for r in data["results"]])
                self._loaded_keys.extend([d[1].key for d in dl if d[0]])

    def load_binary_data(self):
        utf8_key, bytes_key, counter_key = self.get_binary_keys()
        tc = RawStringTranscoder()
        run_in_reactor_thread(self.collection.upsert, utf8_key, "", transcoder=tc)
        self.try_n_times(10, 1, self.collection.get, utf8_key, transcoder=tc)

        tc = RawBinaryTranscoder()
        run_in_reactor_thread(self.collection.upsert, bytes_key, b"", transcoder=tc)
        self.try_n_times(10, 1, self.collection.get, bytes_key, transcoder=tc)

        try:
            run_in_reactor_thread(self.collection.remove, counter_key)
        except CouchbaseException:
            pass

        try:
            self.try_n_times(10, 1, self.collection.get, counter_key)
        except DocumentNotFoundException:
            pass
        except CouchbaseException as ex:
            raise ex

    # @defer.inlineCallbacks
    def purge_data(self):
        # yield defer.DeferredList([self.collection.remove(key) for key in self._loaded_keys])
        for key in self._loaded_keys:
            try:
                run_in_reactor_thread(self.collection.remove, key)
            except CouchbaseException:
                pass

    @defer.inlineCallbacks
    def purge_data1(self):
        yield defer.DeferredList([self.collection.remove(key) for key in self._loaded_keys])

    @defer.inlineCallbacks
    def purge_binary_data(self):
        for k in self.get_binary_keys():
            try:
                yield self.collection.remove(k)
            except CouchbaseException:
                pass

    # def sleep(self, sleep_seconds  # type: float
    #           ) -> None:
    #     d = defer.Deferred()
    #     reactor.callLater(sleep_seconds, d.callback, "")
    #     wait_for_deferred(d)

    # def sleep1(self, sleep_seconds  # type: float
    #            ) -> None:
    #     d = defer.Deferred()
    #     reactor.callLater(sleep_seconds, d.callback, "")
    #     wait_for_deferred1(d)

    # def try_n_times(self,
    #                 num_times,  # type: int
    #                 seconds_between,  # type: Union[int, float]
    #                 func,  # type: Callable
    #                 *args,  # type: Any
    #                 is_deferred=True,  # type: bool
    #                 **kwargs  # type: Any
    #                 ) -> Any:

    #     for _ in range(num_times):
    #         try:
    #             if is_deferred:
    #                 return run_in_reactor_thread(func, *args, **kwargs)
    #             else:
    #                 return func(*args, **kwargs)
    #         # except expected_exceptions as ex:
    #         #     raise ex
    #         except Exception:
    #             time.sleep(seconds_between)
    #             # self.sleep1(seconds_between)

    #     raise CouchbaseTestEnvironmentException(
    #         f"Unsuccessful execution of {func} after {num_times} times, "
    #         "waiting {seconds_between} seconds between calls.")

    def deferred_sleep(self, sleep_seconds  # type: float
                       ) -> None:
        d = defer.Deferred()
        reactor.callLater(sleep_seconds, d.callback, "")
        return d

    # @defer.inlineCallbacks
    def try_n_times(self,
                    num_times,  # type: int
                    seconds_between,  # type: Union[int, float]
                    func,  # type: Callable
                    *args,  # type: Any
                    is_deferred=True,  # type: bool
                    **kwargs  # type: Any
                    ) -> Any:

        for _ in range(num_times):
            try:
                if is_deferred:
                    res = run_in_reactor_thread(func, *args, **kwargs)
                else:
                    res = func(*args, **kwargs)
                return res
            except Exception:
                run_in_reactor_thread(self.deferred_sleep, seconds_between)

        raise CouchbaseTestEnvironmentException(
            f"Unsuccessful execution of {func} after {num_times} times, "
            "waiting {seconds_between} seconds between calls.")

    def try_n_times_till_exception(self,
                                   num_times,  # type: int
                                   seconds_between,  # type: Union[int, float]
                                   func,  # type: Callable
                                   *args,  # type: Any
                                   expected_exceptions=(Exception,),  # type: Tuple[Type[Exception],...]
                                   is_deferred=True,  # type: bool
                                   **kwargs  # type: Any
                                   ):
        # type: (...) -> Any
        for _ in range(num_times):
            try:
                if is_deferred:
                    return run_in_reactor_thread(func, *args, **kwargs)
                else:
                    return func(*args, **kwargs)
            except expected_exceptions:
                # helpful to have this print statement when tests fail
                return
            except Exception:
                raise

        # TODO: option to restart mock server?

        raise CouchbaseTestEnvironmentException(
            f"successful {func} after {num_times} times waiting {seconds_between} seconds between calls.")
