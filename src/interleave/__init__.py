"""
Yield from multiple iterators as values become available

The ``interleave`` package provides a function of the same name that takes a
number of iterators, runs them in separate threads, and yields the values
produced as soon as each one is available.

Visit <https://github.com/jwodder/interleave> for more information.
"""

from __future__ import annotations
from collections.abc import Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import AbstractContextManager, contextmanager
from enum import Enum
from queue import Empty, Queue, SimpleQueue
import sys
from threading import Lock
from time import monotonic
from types import TracebackType
from typing import Any, Generic, NoReturn, Protocol, TypeVar, cast

__version__ = "0.3.0"
__author__ = "John Thorvald Wodder II"
__author_email__ = "interleave@varonathe.org"
__license__ = "MIT"
__url__ = "https://github.com/jwodder/interleave"

__all__ = [
    "DRAIN",
    "EndOfInputError",
    "FINISH_ALL",
    "FINISH_CURRENT",
    "Interleaver",
    "OnError",
    "STOP",
    "interleave",
    "lazy_interleave",
]

T = TypeVar("T")

OnError = Enum("OnError", "STOP DRAIN FINISH_CURRENT FINISH_ALL")
STOP = OnError.STOP
DRAIN = OnError.DRAIN
FINISH_CURRENT = OnError.FINISH_CURRENT
FINISH_ALL = OnError.FINISH_ALL


class QueueProto(Protocol, Generic[T]):
    """
    Protocol for the behavior shared by `queue.Queue` and
    `queue.SimpleQueue` that is of relevance to this package
    """

    def get(self, block: bool = ..., timeout: float | None = ...) -> T: ...

    def put(self, item: T, block: bool = ..., timeout: float | None = ...) -> None: ...


class Result(Generic[T]):
    """
    Container for the result of an operation: either a value or a raised
    exception
    """

    def __init__(
        self,
        value: T | None = None,
        exc_info: (
            tuple[type[BaseException], BaseException, TracebackType] | None
        ) = None,
    ) -> None:
        self.value = value
        self.exc_info = exc_info

    @property
    def success(self) -> bool:
        """
        True if the `Result` represents a value rather than a raised exception
        """
        return self.exc_info is None

    def get(self) -> T:
        """
        If the `Result` represents a value, it is returned; otherwise, the
        represented exception is reraised
        """
        if self.exc_info is None:
            return cast(T, self.value)
        else:
            _, e, tb = self.exc_info
            raise e.with_traceback(tb) from None

    @classmethod
    def for_exc(cls) -> Result[Any]:
        """Construct a `Result` for the exception currently being handled"""
        etype, e, tb = sys.exc_info()
        if etype is None:
            raise ValueError("No exception currently being handled")
        assert etype is not None
        assert e is not None
        assert tb is not None
        return cls(exc_info=(etype, e, tb))


class EndOfInputError(Exception):
    """
    Raised by ``get()`` once all producers have finished and all values have
    been retrieved
    """

    pass


class FunnelQueue(Generic[T]):
    """
    A multi-producer, single-consumer FIFO queue that keeps track of the number
    of active producers and stops outputting values once all producers have
    finished & all input has been retrieved.

    Each new producer must be registered by calling `putting()`, which returns
    a context manager that unregisters the producer on exit.  Once all
    producers are registered, `finalize()` must be called to inform the
    `FunnelQueue` of this.
    """

    def __init__(self, queue_size: int | None = None) -> None:
        """
        :param queue_size:
            Sets the maximum size of the internal queue.  When `None` (the
            default), `FunnelQueue` uses a `queue.SimpleQueue`, which has no
            maximum size.  When non-`None` (including zero, signifying no
            maximum size), `FunnelQueue` uses a `queue.Queue`, whose ``get()``
            method is uninterruptible (including by `KeyboardInterrupt`) on
            Windows.
        """
        self.queue: QueueProto[T]
        if queue_size is None:
            self.queue = SimpleQueue()
        else:
            self.queue = Queue(queue_size)
        self.producer_qty = 0
        self.all_submitted = False
        self.lock = Lock()
        self.done_sentinel = object()
        self.done = False

    def putting(self) -> AbstractContextManager[None]:
        """
        Increment the producer count and return a context manager that
        decrements the count on exit

        :raises ValueError:
            if `finalize()` was previously called on the `FunnelQueue`
        """
        with self.lock:
            if self.all_submitted:
                raise ValueError(
                    "Cannot submit new producers after finalize() is called"
                )
            self.producer_qty += 1
        return self._put_ctx()

    @contextmanager
    def _put_ctx(self) -> Iterator[None]:
        try:
            yield
        finally:
            self.decrement()

    def finalize(self) -> None:
        """
        Notify the `FunnelQueue` that all producers have been registered.
        Calling `putting()` after this will result in a `ValueError`.
        """
        with self.lock:
            if not self.all_submitted:
                self.all_submitted = True
                if self.producer_qty == 0:
                    self.put(cast(T, self.done_sentinel))

    def decrement(self) -> None:
        """
        Forcibly decrement the producer count.  This is useful if a registered
        producer was cancelled before starting.
        """
        with self.lock:
            self.producer_qty -= 1
            if self.producer_qty == 0 and self.all_submitted:
                self.put(cast(T, self.done_sentinel))

    def put(self, value: T) -> None:
        if self.done:
            raise ValueError("Funnel is closed for business")
        self.queue.put(value)

    def get(self, block: bool = True, timeout: float | None = None) -> T:
        if self.done:
            raise EndOfInputError()
        x = self.queue.get(block=block, timeout=timeout)
        if x is self.done_sentinel:
            self.done = True
            raise EndOfInputError()
        else:
            return x


class Interleaver(Generic[T]):
    """
    An iterator and context manager.  As an iterator, it yields the values
    generated by the iterators passed to the corresponding `interleave()` call
    as they become available.  As a context manager, it returns itself on entry
    and, on exit, cleans up any unfinished threads by calling the
    ``shutdown(wait=True)`` method (see below).

    An `Interleaver` can be instantiated either by calling `interleave()` or by
    calling the constructor directly.  The constructor takes the same arguments
    as `interleave()`, minus ``iterators``, and produces a new `Interleaver`
    that is not yet running any iterators.  Iterators are submitted to a new
    `Interleaver` via the `submit()` method; once all desired iterators have
    been submitted, the `finalize()` method **must** be called so that the
    `Interleaver` can tell when everything's finished.

    An `Interleaver` will shut down its `ThreadPoolExecutor` and wait for the
    threads to finish after yielding its final value (specifically, when a call
    is made to ``__next__``/`get()` that would result in `StopIteration` or
    another exception being raised).  In the event that an `Interleaver` is
    abandoned before iteration completes, the associated resources may not be
    properly cleaned up, and threads may continue running indefinitely.  For
    this reason, it is strongly recommended that you wrap any iteration over an
    `Interleaver` in the context manager in order to handle a premature end to
    iteration (including from a `KeyboardInterrupt`).
    """

    def __init__(
        self,
        max_workers: int | None = None,
        thread_name_prefix: str = "",
        queue_size: int | None = None,
        onerror: OnError = STOP,
    ) -> None:
        self._funnel: FunnelQueue[Result[T]] = FunnelQueue(queue_size)
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix=thread_name_prefix
        )
        self._onerror = onerror
        self._futures: list[Future[None]] = []
        self._done_flag = False
        self._error: Result[T] | None = None
        self._exhausted = False

    def _process(self, ctx: AbstractContextManager[None], it: Iterator[T]) -> None:
        with ctx:
            while not self._done_flag:
                try:
                    x = next(it)
                except StopIteration:
                    return
                # According to various sources, only the main thread can
                # receive a KeyboardInterrupt, so there's no point in trying to
                # catch one here.
                except Exception:
                    self._funnel.put(Result.for_exc())
                    return
                else:
                    self._funnel.put(Result(x))

    def _process_input(
        self, ctx: AbstractContextManager[None], it: Iterator[Iterator[T]]
    ) -> None:
        with ctx:
            try:
                while not self._done_flag:
                    try:
                        x = next(it)
                    except StopIteration:
                        return
                    # According to various sources, only the main thread can
                    # receive a KeyboardInterrupt, so there's no point in trying to
                    # catch one here.
                    except Exception:
                        self._funnel.put(Result.for_exc())
                        return
                    else:
                        self.submit(x)
            finally:
                self.finalize()

    def submit(self, it: Iterator[T]) -> None:
        """
        .. versionadded:: 0.2.0

        Add an iterator to the `Interleaver`.

        If the `Interleaver` was returned from `interleave()` or has already
        had `finalize()` called on it, calling `submit()` will result in a
        `ValueError`.
        """
        # The funnel's producer count needs to be incremented outside of
        # `_process()` so that the increment happens immediately rather than
        # being delayed until the thread actually starts.  Without this, if an
        # initial batch of threads were to all register, finish, & unregister
        # while all remaining threads had yet to be started, the funnel would
        # then see that there were no producers left and assume that everything
        # was finished, leading to a premature `EndOfInputError`.
        self._futures.append(
            self._pool.submit(self._process, self._funnel.putting(), it)
        )

    def _submit_input(self, it: Iterable[Iterator[T]]) -> None:
        self._futures.append(
            self._pool.submit(self._process_input, self._funnel.putting(), iter(it))
        )

    def finalize(self) -> None:
        """
        .. versionadded:: 0.2.0

        Notify the `Interleaver` that all iterators have been registered.  This
        method must be called in order for the `Interleaver` to detect the end
        of iteration; if this method has not been called and all submitted
        iterators have finished & had their values retrieved, then a subsequent
        call to ``next(it)`` will end up hanging indefinitely.
        """
        # Tell the funnel that all producers have been initialized and there
        # will not be any more.  Without this, if the first producer was
        # registered, finished, and unregistered before any further producers
        # were registered (i.e., if the first `_process()` thread ran &
        # completed as soon as it was submitted, finishing before the second
        # call to `_funnel.putting()` even happened), the funnel would then see
        # that there were no producers left and assume that everything was
        # finished, leading to a premature `EndOfInputError`.
        self._funnel.finalize()
        # (An alternative to this system would be to pass the total number of
        # iterators/producers to the `FunnelQueue` constructor, but then we
        # wouldn't be able to support submitting new iterators to the batch.)

    def __enter__(self) -> Interleaver[T]:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        self.shutdown(wait=True)

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        try:
            return self.get()
        except EndOfInputError:
            raise StopIteration

    def get(self, block: bool = True, timeout: float | None = None) -> T:
        """
        .. versionadded:: 0.2.0

        Fetch the next value generated by the iterators.  If all iterators have
        finished and all values have been retrieved, raises
        `interleaver.EndOfInputError`.  If ``block`` is ``False`` and no values
        are immediately available, raises `queue.Empty`.  If ``block`` is
        `True`, waits up to ``timeout`` seconds (or indefinitely, if
        ``timeout`` is `None`) for the next value to become available or for
        all iterators to end; if nothing happens before the timeout expires,
        raises `queue.Empty`.

        ``it.get(block=True, timeout=None)`` is equivalent to ``next(it)``,
        except that the latter converts an `EndOfInputError` to
        `StopIteration`.

        .. note::

            When ``onerror=STOP`` and a timeout is set, if an iterator raises
            an exception, the timeout may be exceeded as the `Interleaver`
            waits for all remaining threads to shut down.
        """
        if self._exhausted:
            raise EndOfInputError()
        while True:
            start_time = monotonic()
            try:
                r = self._funnel.get(block=block, timeout=timeout)
            except EndOfInputError:
                self._end()
            else:
                if r.success:
                    return r.get()
                elif self._error is None:
                    self._error = r
                    self.shutdown(wait=False, _mode=self._onerror)
                    if self._onerror is STOP:
                        self._end()
                if timeout is not None:
                    timeout -= monotonic() - start_time
                    if timeout <= 0:  # pragma: no cover
                        raise Empty()

    def _end(self) -> NoReturn:
        self._exhausted = True
        self._pool.shutdown(wait=True)
        if self._error is not None:
            e, self._error = self._error, None
            assert not e.success
            e.get()
            raise AssertionError("Unreachable")  # pragma: no cover
        raise EndOfInputError()

    def shutdown(self, wait: bool = True, *, _mode: OnError = STOP) -> None:
        """
        Call `finalize()` if it hasn't been called yet, tell all running
        iterators to stop iterating, cancel any outstanding iterators that
        haven't been started yet, and shut down the `ThreadPoolExecutor`.  The
        ``wait`` parameter is passed through to the call to
        ``ThreadPoolExecutor.shutdown()``.

        The `Interleaver` can continue to be iterated over after calling
        `shutdown()` and will yield any remaining values produced by the
        iterators before they stopped completely.
        """
        self.finalize()
        if _mode in (STOP, DRAIN):
            self._done_flag = True
        if _mode is not FINISH_ALL:
            for f in self._futures:
                if f.cancel():
                    self._funnel.decrement()
        self._pool.shutdown(wait=wait)


def interleave(
    iterators: Iterable[Iterator[T]],
    *,
    max_workers: int | None = None,
    thread_name_prefix: str = "",
    queue_size: int | None = None,
    onerror: OnError = STOP,
) -> Interleaver[T]:
    """
    Run the given iterators in separate threads and return an iterator that
    yields the values yielded by them as they become available.

    Note that ``iterators`` (but not its elements) is fully evaluated &
    consumed before `interleave()` returns.  If you instead want ``iterators``
    to be evaluated concurrently with the iterators themselves, see
    `lazy_interleave()`.

    The ``max_workers`` and ``thread_name_prefix`` parameters are passed
    through to the underlying `concurrent.futures.ThreadPoolExecutor` (q.v.).
    ``max_workers`` determines the maximum number of iterators to run at one
    time.

    The ``queue_size`` parameter sets the maximum size of the queue used
    internally to pipe values yielded by the iterators; when the queue is full,
    any iterator with a value to yield will block waiting for the next value to
    be dequeued by a call to the interleaver's ``__next__``.  When
    ``queue_size`` is `None` (the default), `interleave()` uses a
    `queue.SimpleQueue`, which has no maximum size.  When ``queue_size`` is
    non-`None` (including zero, signifying no maximum size), `interleave()`
    uses a `queue.Queue`, whose ``get()`` method is uninterruptible (including
    by `KeyboardInterrupt`) on Windows.

    The ``onerror`` parameter is an enum that determines how `interleave()`
    should behave if one of the iterators raises an exception.  The possible
    values are:

    ``STOP``
        *(default)* Stop iterating over all iterators, cancel any outstanding
        iterators that haven't been started yet, wait for all running threads
        to finish, and reraise the exception.  Note that, due to the inability
        to stop an iterator between yields, the "waiting" step involves waiting
        for each currently-running iterator to yield its next value before
        stopping.  This can deadlock if the queue fills up in the interim.

    ``DRAIN``
        Like ``STOP``, but any remaining values yielded by the iterators before
        they finish are yielded by the interleaver before raising the exception

    ``FINISH_ALL``
        Continue running as normal and reraise the exception once all iterators
        have finished

    ``FINISH_CURRENT``
        Like ``FINISH_ALL``, except that only currently-running iterators are
        run to completion; any iterators whose threads haven't yet been started
        when the exception is raised will have their jobs cancelled

    Regardless of the value of ``onerror``, any later exceptions raised by
    iterators after the initial exception are discarded.
    """

    ilvr: Interleaver[T] = Interleaver(
        max_workers=max_workers,
        thread_name_prefix=thread_name_prefix,
        queue_size=queue_size,
        onerror=onerror,
    )
    for it in iterators:
        ilvr.submit(it)
    ilvr.finalize()
    return ilvr


def lazy_interleave(
    iterators: Iterable[Iterator[T]],
    *,
    max_workers: int | None = None,
    thread_name_prefix: str = "",
    queue_size: int | None = None,
    onerror: OnError = STOP,
) -> Interleaver[T]:
    """
    .. versionadded:: 0.3.0

    Like `interleave()`, but instead of fully evaluating ``iterators``
    immediately, it is iterated over in a thread in the thread pool, and as
    each iterator is produced, it is submitted to the `Interleaver`
    concurrently with the `Interleaver` iterating over the other iterators
    already produced.  This is useful if the creation of the iterators
    themselves is nontrivial and involves work that could be done currently
    with the iterators themselves.

    Note that the ``iterators``-evaluation thread is handled the same way as
    other threads when it comes to error handling: an exception occurring in
    one of the iterator threads may (depending on the value of ``onerror``)
    cause the ``iterators`` thread to be stopped, and if
    ``next(iter(iterators))`` raises an exception, it may cause all other
    threads to be stopped.
    """
    ilvr: Interleaver[T] = Interleaver(
        max_workers=max_workers,
        thread_name_prefix=thread_name_prefix,
        queue_size=queue_size,
        onerror=onerror,
    )
    ilvr._submit_input(iterators)
    return ilvr
