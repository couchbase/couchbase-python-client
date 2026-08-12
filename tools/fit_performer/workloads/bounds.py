from __future__ import annotations

import multiprocessing as mp
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any

from ..generated.shared import bounds_pb2 as bounds_pb


class Counters:
    """Registry of named counters, shared across workloads and run requests.

    The performer owns a single ``Counters`` instance for its lifetime.  ``setCounter`` /
    ``clearAllCounters`` mutate it directly, and it is passed down into whichever executor a run
    builds.  Counters therefore persist across runs until explicitly overwritten or cleared.

    The backing store matches the concurrency mechanism in use:

    * multi-threading (the default): a plain ``dict`` + ``threading.Lock``.  Workers are threads
      in this process, so no IPC is involved on the per-iteration bounds check.
    * multi-processing: ``enable_cross_process_sharing`` upgrades the store to a
      ``multiprocessing.Manager().dict()`` + ``Manager().Lock()`` whose proxies are shared with
      the forkserver worker processes.

    A performer only ever uses one concurrency mechanism, so the upgrade (if any) happens once,
    before any worker is spawned.  The ``Manager`` itself stays in this (parent) process: it is
    dropped from the pickled state (see ``__getstate__``) so workers receive only the picklable
    dict / lock proxies.
    """

    def __init__(self) -> None:
        # In-process backing by default: correct and fast for multi-threading, and for the
        # setCounter / clearAllCounters / counter_eq coordination, which all run in this process.
        self._counters = {}
        self._lock = threading.Lock()
        self._manager = None

    def enable_cross_process_sharing(self) -> None:
        """Upgrade the backing to a ``multiprocessing.Manager`` so counters are visible to the
        forkserver worker processes.  Called by the multi-process executor before it spawns its
        pool.  Idempotent, and preserves any counters already created.
        """
        if self._manager is not None:
            return
        self._manager = mp.get_context('forkserver').Manager()
        shared = self._manager.dict()
        shared.update(self._counters)
        self._counters = shared
        self._lock = self._manager.Lock()

    def shutdown(self) -> None:
        """Release the ``Manager`` subprocess (if one was created) and fall back to in-process
        backing.  Counter values are preserved.  A no-op when no Manager is in use.
        """
        if self._manager is None:
            return
        preserved = dict(self._counters)
        self._manager.shutdown()
        self._manager = None
        self._counters = preserved
        self._lock = threading.Lock()

    def __getstate__(self):
        # The Manager lives in the parent process only; workers coordinate through the dict / lock
        # proxies, which are picklable.  The SyncManager object itself is not, so drop it.
        state = self.__dict__.copy()
        state['_manager'] = None
        return state

    def _check_global_counter(self, shared_counter: bounds_pb.Counter) -> None:
        counter_type = shared_counter.WhichOneof('counter')
        if counter_type != 'global':
            raise ValueError(f"Counter type '{counter_type}' not recognized")

    def _get_global_count(self, shared_counter: bounds_pb.Counter) -> int:
        self._check_global_counter(shared_counter)
        # `global` is a Python keyword, so the generated field can only be read via getattr.
        return getattr(shared_counter, 'global').count

    def _ensure_created(self, shared_counter: bounds_pb.Counter) -> str:
        """Create the counter at its initial value if it does not already exist.

        Must be called while holding ``self._lock``.  Returns the counter id.
        """
        counter_id = shared_counter.counter_id
        if counter_id not in self._counters:
            self._counters[counter_id] = self._get_global_count(shared_counter)
        return counter_id

    def set(self, shared_counter: bounds_pb.Counter) -> None:
        """Force the counter to a new value, creating it if necessary (setCounter RPC)."""
        counter_id = shared_counter.counter_id
        with self._lock:
            self._counters[counter_id] = self._get_global_count(shared_counter)

    def get(self, shared_counter: bounds_pb.Counter) -> int:
        """Return the counter's current value, creating it at its initial value first if needed."""
        with self._lock:
            counter_id = self._ensure_created(shared_counter)
            return self._counters[counter_id]

    def decrement_and_get(self, shared_counter: bounds_pb.Counter) -> int:
        """Decrement and return the counter, creating it at its initial value first if needed."""
        with self._lock:
            counter_id = self._ensure_created(shared_counter)
            self._counters[counter_id] -= 1
            return self._counters[counter_id]

    def increment_and_get(self, shared_counter: bounds_pb.Counter) -> int:
        """Increment and return the counter, creating it at its initial value first if needed."""
        with self._lock:
            counter_id = self._ensure_created(shared_counter)
            self._counters[counter_id] += 1
            return self._counters[counter_id]

    def clear(self) -> None:
        """Remove all counters (clearAllCounters RPC)."""
        with self._lock:
            self._counters.clear()


class BoundsExecutor(ABC):
    """Decides, before each workload iteration, whether the workload may run again."""

    @abstractmethod
    def can_execute(self) -> bool:
        raise NotImplementedError('can_execute must be implemented by concrete class.')


class CounterBoundsExecutor(BoundsExecutor):
    """Runs while the shared counter, decremented on each check, remains >= 0."""

    def __init__(self, counters: Counters, shared_counter: bounds_pb.Counter) -> None:
        self._counters = counters
        self._shared_counter = shared_counter

    def can_execute(self) -> bool:
        return self._counters.decrement_and_get(self._shared_counter) >= 0


class CounterEqualsBoundsExecutor(BoundsExecutor):
    """Runs while the shared counter still equals the value it held when bounds were set.

    The counter is not modified here; another workload (or a setCounter RPC) changes it to
    stop this workload.  See the ``counter_eq`` note in shared.bounds.proto.
    """

    def __init__(self, counters: Counters, shared_counter: bounds_pb.Counter) -> None:
        self._counters = counters
        self._shared_counter = shared_counter
        self._initial_value = counters.get(shared_counter)

    def can_execute(self) -> bool:
        return self._counters.get(self._shared_counter) == self._initial_value


class TimeBoundsExecutor(BoundsExecutor):
    """Runs until a wall-clock deadline is reached."""

    def __init__(self, deadline: datetime) -> None:
        self._deadline = deadline

    def can_execute(self) -> bool:
        return datetime.now() < self._deadline


class SimpleBoundsExecutor(BoundsExecutor):
    """No bounds supplied: run each command in the workload exactly once."""

    def __init__(self, command_count: int) -> None:
        self._remaining = command_count

    def can_execute(self) -> bool:
        self._remaining -= 1
        return self._remaining >= 0


def build_bounds_executor(workload: Any,
                          counters: Counters
                          ) -> BoundsExecutor:
    """Build the BoundsExecutor for a workload from its protobuf bounds.

    Args:
        workload (protobuf Workload): The gRPC workload object.
        counters (Counters): The performer-level counter registry, used for counter /
            counter_eq bounds.

    Raises:
        ValueError: If the bounds type (or counter type) is not recognized.
    """
    if not workload.HasField('bounds'):
        # No bounds: execute each command in the workload once.
        return SimpleBoundsExecutor(len(workload.command))

    bounds = workload.bounds
    bounds_type = bounds.WhichOneof('bounds')
    if bounds_type == 'counter':
        return CounterBoundsExecutor(counters, bounds.counter)
    if bounds_type == 'counter_eq':
        return CounterEqualsBoundsExecutor(counters, bounds.counter_eq)
    if bounds_type == 'for_time':
        deadline = datetime.now() + timedelta(seconds=bounds.for_time.seconds)
        return TimeBoundsExecutor(deadline)
    raise ValueError(f"Bounds type '{bounds_type}' not recognized")
