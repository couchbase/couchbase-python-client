from typing import Union, Iterable, Any

from couchbase_core import StopAsyncIteration

IterableQuery = Iterable[Any]


class IterableWrapper(object):
    def __init__(self,
                 parent  # type: IterableQuery
                 ):
        self.done = False
        self.buffered_rows = []
        self.parent = parent  # type: IterableQuery

    def metadata(self):
        # type: (...) -> JSON
        return self.parent.meta

    def __iter__(self):
        for row in self.buffered_rows:
            yield row
        parent_iter = iter(self.parent)
        while not self.done:
            try:
                next_item = next(parent_iter)
                self.buffered_rows.append(next_item)
                yield next_item
            except (StopAsyncIteration, StopIteration) as e:
                self.done = True
                break