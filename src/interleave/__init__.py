"""
Yield from multiple iterators as values become available

Visit <https://github.com/jwodder/interleave> for more information.
"""

from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from enum import Enum
from queue import Queue, SimpleQueue
import sys
from threading import Event, Lock
from types import TracebackType
from typing import (
    Any,
    ContextManager,
    Generic,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    Type,
    TypeVar,
    cast,
)

if sys.version_info[:2] >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol

__version__ = "0.1.0.dev1"
__author__ = "John Thorvald Wodder II"
__author_email__ = "interleave@varonathe.org"
__license__ = "MIT"
__url__ = "https://github.com/jwodder/interleave"

__all__ = [
    "DRAIN",
    "FINISH_ALL",
    "FINISH_CURRENT",
    "OnError",
    "STOP",
    "interleave",
]

ExcInfo = Tuple[Type[BaseException], BaseException, TracebackType]

T = TypeVar("T")

OnError = Enum("OnError", "STOP DRAIN FINISH_CURRENT FINISH_ALL")
STOP = OnError.STOP
DRAIN = OnError.DRAIN
FINISH_CURRENT = OnError.FINISH_CURRENT
FINISH_ALL = OnError.FINISH_ALL


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


class QueueProto(Protocol, Generic[T]):
    """
    Protocol for the behavior shared by queue.Queue and queue.SimpleQueue that
    is of relevance to this package
    """

    def get(self, block: bool = ..., timeout: Optional[float] = ...) -> T:
        ...

    def put(self, item: T, block: bool = ..., timeout: Optional[float] = ...) -> None:
        ...


class EndOfInputError(Exception):
    pass


class FunnelQueue(Generic[T]):
    def __init__(self, queue_size: Optional[int] = None) -> None:
        self.queue: QueueProto[T]
        if queue_size is None:
            self.queue = SimpleQueue()
        else:
            self.queue = Queue(queue_size)
        self.producer_qty = 0
        self.all_submitted = False
        self.lock = Lock()
        self.done_sentinel = object()

    def putting(self) -> ContextManager[None]:
        with self.lock:
            if self.all_submitted:
                raise ValueError(
                    "Cannot submit new producers after finalize() is called"
                )
            self.producer_qty += 1
        return self._close_one()

    @contextmanager
    def _close_one(self) -> Iterator[None]:
        try:
            yield
        finally:
            self.decrement()

    def finalize(self) -> None:
        with self.lock:
            self.all_submitted = True
            if self.producer_qty == 0:
                self.put(cast(T, self.done_sentinel))

    def decrement(self) -> None:
        with self.lock:
            self.producer_qty -= 1
            if self.producer_qty == 0 and self.all_submitted:
                self.put(cast(T, self.done_sentinel))

    def put(self, value: T) -> None:
        self.queue.put(value)

    def get(self) -> T:
        x = self.queue.get()
        if x is self.done_sentinel:
            raise EndOfInputError()
        else:
            return x


def interleave(
    iterators: Iterable[Iterator[T]],
    *,
    max_workers: Optional[int] = None,
    thread_name_prefix: str = "",
    queue_size: Optional[int] = None,
    onerror: OnError = STOP,
) -> Iterator[T]:
    funnel: FunnelQueue[Result[T]] = FunnelQueue(queue_size)
    done_flag = Event()

    def process(ctx: ContextManager[None], it: Iterator[T]) -> None:
        with ctx:
            while not done_flag.is_set():
                try:
                    x = next(it)
                except StopIteration:
                    return
                # According to various sources, only the main thread can
                # receive a KeyboardInterrupt, so there's no point in trying to
                # catch one here.
                except Exception:
                    funnel.put(Result.for_exc())
                    return
                else:
                    funnel.put(Result(x))

    with ThreadPoolExecutor(
        max_workers=max_workers, thread_name_prefix=thread_name_prefix
    ) as pool:

        # The funnel's producer count needs to be incremented outside of
        # `process()` so that the increment happens immediately rather than
        # being delayed until the thread actually starts.  Without this, if an
        # initial batch of threads were to all register, finish, & unregister
        # while all remaining threads had yet to be started, the funnel would
        # then see that there were no producers left and assume that everything
        # was finished, leading to a premature `EndOfInputError`.

        futures = [pool.submit(process, funnel.putting(), it) for it in iterators]

        # Tell the funnel that all producers have been initialized and there
        # will not be any more.  Without this, if the first producer was
        # registered, finished, and unregistered before any further producers
        # were registered (i.e., if the first `process()` thread ran &
        # completed as soon as it was submitted, finishing before the second
        # call to `funnel.putting()` even happened), the funnel would then see
        # that there were no producers left and assume that everything was
        # finished, leading to a premature `EndOfInputError`.

        funnel.finalize()

        # (An alternative to this system would be to pass the total number of
        # iterators/producers to the `FunnelQueue` constructor, but then we
        # wouldn't be able to support submitting new iterators to the batch,
        # which may or may not become an eventual feature.)

        error: Optional[Result[T]] = None
        while True:
            try:
                r = funnel.get()
            except EndOfInputError:
                break
            else:
                if r.success:
                    yield r.get()
                elif error is None:
                    error = r
                    if onerror is not FINISH_ALL:
                        for f in futures:
                            if f.cancel():
                                funnel.decrement()
                    if onerror in (STOP, DRAIN):
                        done_flag.set()
                    if onerror is STOP:
                        break
        if error is not None:
            assert not error.success
            error.get()
