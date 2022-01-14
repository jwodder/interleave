"""
Yield from multiple iterators as values become available

Visit <https://github.com/jwodder/interleave> for more information.
"""

from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue
import sys
from types import TracebackType
from typing import (
    Any,
    Generic,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    Type,
    TypeVar,
    cast,
)

__version__ = "0.1.0.dev1"
__author__ = "John Thorvald Wodder II"
__author_email__ = "interleave@varonathe.org"
__license__ = "MIT"
__url__ = "https://github.com/jwodder/interleave"

__all__ = ["interleave"]

T = TypeVar("T")

ExcInfo = Tuple[Type[BaseException], BaseException, TracebackType]


class Result(Generic[T]):
    def __init__(self, value: Optional[T] = None, exc_info: Optional[ExcInfo] = None):
        self.value = value
        self.exc_info = exc_info

    @property
    def success(self) -> bool:
        return self.exc_info is None

    def get(self) -> T:
        if self.exc_info is None:
            return cast(T, self.value)
        else:
            _, e, tb = self.exc_info
            raise e.with_traceback(tb)

    @classmethod
    def for_exc(cls) -> Result[Any]:
        etype, e, tb = sys.exc_info()
        if etype is None:
            raise ValueError("No exception currently being handled")
        assert etype is not None
        assert e is not None
        assert tb is not None
        return cls(exc_info=(etype, e, tb))


def interleave(
    iterators: Iterable[Iterator[T]],
    *,
    max_workers: Optional[int] = None,
    queue_size: int = 0,
    queue_wait: float = 1.0
) -> Iterator[T]:
    pool = ThreadPoolExecutor(max_workers=max_workers)
    queue: Queue[Result[T]] = Queue(queue_size)
    futures = []

    def run_iterator(it: Iterator[T]) -> None:
        while True:
            try:
                x = next(it)
            except StopIteration:
                return
            except BaseException:
                queue.put(Result.for_exc())
                return
            else:
                queue.put(Result(x))

    for it in iterators:
        futures.append(pool.submit(run_iterator, it))

    while True:
        try:
            # We need to set a timeout so that keyboard interrupts aren't
            # ignored on Windows
            r = queue.get(timeout=queue_wait)
        except Empty:
            if all(f.done() for f in futures):
                break
        else:
            yield r.get()
