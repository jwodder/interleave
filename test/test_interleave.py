from __future__ import annotations
from collections.abc import Callable, Iterator, Sequence
from itertools import count
from math import isclose
import os
from pathlib import Path
from queue import Empty
from signal import SIGINT
from subprocess import PIPE, Popen
import sys
from threading import active_count
from time import monotonic, sleep
import traceback
from typing import Any, Optional
from unittest.mock import MagicMock, call
import pytest
from interleave import (
    DRAIN,
    FINISH_ALL,
    FINISH_CURRENT,
    STOP,
    EndOfInputError,
    Interleaver,
    OnError,
    interleave,
)

CI = "CI" in os.environ

UNIT = 1.0 if CI else 0.25

pytestmark = pytest.mark.flaky(reruns=5, condition=CI)


def sleeper(
    tid: int,
    delays: Sequence[int | str],
    done_callback: Optional[Callable[[int], Any]] = None,
) -> Iterator[tuple[int, int]]:
    for i, d in enumerate(delays):
        if isinstance(d, int):
            sleep(d * UNIT)
            yield (tid, i)
        else:
            if done_callback is not None:
                done_callback(tid)
            raise RuntimeError(d)
    # We're not putting this under a `finally:` block because (on macOS 11.6
    # Intel with Python 3.9.9 and 3.10.1, at least) doing so would mean it
    # would fire if & when `cancel()` is called on the corresponding future
    # while it's running (in which case a GeneratorExit gets raised in this
    # function), but only if the iterable of iterators passed to `interleave()`
    # was an iterator rather than a list.  I have been unable to write an MVCE
    # that reproduces this behavior, and I'm not sure if it's even worth
    # looking into.
    if done_callback is not None:
        done_callback(tid)


def test_simple() -> None:
    INTERVALS = [
        (0, 1, 2),
        (2, 2, 2),
        (5, 2, 1),
    ]
    threads = active_count()
    cb = MagicMock()
    with interleave(
        sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS)
    ) as it:
        assert list(it) == [
            (0, 0),
            (0, 1),
            (1, 0),
            (0, 2),
            (1, 1),
            (2, 0),
            (1, 2),
            (2, 1),
            (2, 2),
        ]
        assert active_count() == threads
        assert cb.call_args_list == [call(i) for i in range(len(INTERVALS))]


def test_timing() -> None:
    INTERVALS = [
        (0, 1, 2),
        (2, 2, 2),
        (5, 2, 1),
    ]
    prev: Optional[float] = None
    with interleave(
        sleeper(i, intervals) for i, intervals in enumerate(INTERVALS)
    ) as it:
        for _ in it:
            now = monotonic()
            if prev is not None:
                assert isclose(now - prev, UNIT, rel_tol=0.3, abs_tol=0.1)
            prev = now


def test_no_iterators() -> None:
    it: Iterator[Any]
    with interleave([]) as it:
        with pytest.raises(StopIteration):
            next(it)


def test_ragged() -> None:
    INTERVALS = [
        (0, 1, 2, 3, 3),
        (2, 2, 3),
        (5, 3),
    ]
    with interleave(
        sleeper(i, intervals) for i, intervals in enumerate(INTERVALS)
    ) as it:
        assert list(it) == [
            (0, 0),
            (0, 1),
            (1, 0),
            (0, 2),
            (1, 1),
            (2, 0),
            (0, 3),
            (1, 2),
            (2, 1),
            (0, 4),
        ]


def test_shrinking_ragged() -> None:
    INTERVALS = [
        (0, 1, 2, 3),
        (2, 2, 3),
        (5, 3),
        (9,),
        (),
    ]
    with interleave(
        sleeper(i, intervals) for i, intervals in enumerate(INTERVALS)
    ) as it:
        assert list(it) == [
            (0, 0),
            (0, 1),
            (1, 0),
            (0, 2),
            (1, 1),
            (2, 0),
            (0, 3),
            (1, 2),
            (2, 1),
            (3, 0),
        ]


def test_growing_ragged() -> None:
    INTERVALS = [
        (),
        (0,),
        (1, 1),
        (3, 1, 2),
        (5, 2, 1, 1),
    ]
    with interleave(
        sleeper(i, intervals) for i, intervals in enumerate(INTERVALS)
    ) as it:
        assert list(it) == [
            (1, 0),
            (2, 0),
            (2, 1),
            (3, 0),
            (3, 1),
            (4, 0),
            (3, 2),
            (4, 1),
            (4, 2),
            (4, 3),
        ]


def test_error() -> None:
    INTERVALS: list[tuple[int | str, ...]] = [
        (0, 1, 2),
        (2, 2, "This is an error.", "This is not raised."),
        (5, "This is not seen.", 1),
    ]
    threads = active_count()
    cb = MagicMock()
    with interleave(
        sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS)
    ) as it:
        for expected in [(0, 0), (0, 1), (1, 0), (0, 2), (1, 1)]:
            assert next(it) == expected
        with pytest.raises(RuntimeError) as excinfo:
            next(it)
        with pytest.raises(StopIteration):
            next(it)
        with pytest.raises(StopIteration):
            next(it)
        assert str(excinfo.value) == "This is an error."
        assert active_count() == threads
        assert cb.call_args_list == [call(0), call(1)]


def test_error_sized_queue() -> None:
    INTERVALS: list[tuple[int | str, ...]] = [
        (0, 1, 2),
        (2, 2, "This is an error.", "This is not raised."),
        (5, "This is not seen.", 1),
    ]

    def queue_spam(tid: int) -> Iterator[tuple[int, int]]:
        sleep(6 * UNIT)
        for i in count():
            yield (tid, i)

    threads = active_count()
    cb = MagicMock()
    with interleave(
        [sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS)]
        + [queue_spam(len(INTERVALS))],
        queue_size=4,
    ) as it:
        for expected in [(0, 0), (0, 1), (1, 0), (0, 2), (1, 1)]:
            assert next(it) == expected
        with pytest.raises(RuntimeError) as excinfo:
            next(it)
        assert str(excinfo.value) == "This is an error."
        assert active_count() == threads
        assert cb.call_args_list == [call(0), call(1)]


def test_finish_current() -> None:
    INTERVALS: list[tuple[int | str, ...]] = [
        (0, 1, 2),
        (2, 2, "This is an error."),
        (5, 1, 3),
        (8, "This error is discarded."),
    ]
    threads = active_count()
    cb = MagicMock()
    with interleave(
        [sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS)],
        onerror=FINISH_CURRENT,
    ) as it:
        for expected in [
            (0, 0),
            (0, 1),
            (1, 0),
            (0, 2),
            (1, 1),
            (2, 0),
            (2, 1),
            (3, 0),
            (2, 2),
        ]:
            assert next(it) == expected
        with pytest.raises(RuntimeError) as excinfo:
            next(it)
        assert str(excinfo.value) == "This is an error."
        assert active_count() == threads
        assert cb.call_args_list == [call(0), call(1), call(3), call(2)]


def test_max_workers() -> None:
    INTERVALS = [
        (0, 1, 2, 3),
        (2, 2),
        (5, 3, 3),
        (9, 3),
        (3, 3),
    ]
    threads = active_count()
    cb = MagicMock()
    with interleave(
        [sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS)],
        max_workers=4,
    ) as it:
        assert list(it) == [
            (0, 0),
            (0, 1),
            (1, 0),
            (0, 2),
            (1, 1),
            (2, 0),
            (0, 3),
            (4, 0),
            (2, 1),
            (3, 0),
            (4, 1),
            (2, 2),
            (3, 1),
        ]
        assert active_count() == threads
        assert cb.call_args_list == [call(1), call(0), call(4), call(2), call(3)]


def test_finish_current_max_workers() -> None:
    INTERVALS: list[tuple[int | str, ...]] = [
        (0, 1, 2, "This is an error."),
        (2, 2),
        (5, 2, 3),
        (8, 3),
        (3, 3),
        (0, 1, 2, 3),
    ]
    threads = active_count()
    cb = MagicMock()
    with interleave(
        [sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS)],
        max_workers=4,
        onerror=FINISH_CURRENT,
    ) as it:
        for expected in [
            (0, 0),
            (0, 1),
            (1, 0),
            (0, 2),
            (1, 1),
            (2, 0),
            (4, 0),
            (2, 1),
            (3, 0),
            (4, 1),
            (2, 2),
            (3, 1),
        ]:
            assert next(it) == expected
        with pytest.raises(RuntimeError) as excinfo:
            next(it)
        assert str(excinfo.value) == "This is an error."
        assert active_count() == threads
        assert cb.call_args_list == [call(0), call(1), call(4), call(2), call(3)]


def test_finish_all_max_workers() -> None:
    INTERVALS: list[tuple[int | str, ...]] = [
        (0, 1, 2, "This is an error."),
        (2, 2),
        (5, 3),
        (9, 3),
        (3, 4),
        (3, 4),
    ]
    threads = active_count()
    cb = MagicMock()
    with interleave(
        [sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS)],
        max_workers=4,
        onerror=FINISH_ALL,
    ) as it:
        for expected in [
            (0, 0),
            (0, 1),
            (1, 0),
            (0, 2),
            (1, 1),
            (2, 0),
            (4, 0),
            (5, 0),
            (2, 1),
            (3, 0),
            (4, 1),
            (5, 1),
            (3, 1),
        ]:
            assert next(it) == expected
        with pytest.raises(RuntimeError) as excinfo:
            next(it)
        assert str(excinfo.value) == "This is an error."
        assert active_count() == threads
        assert cb.call_args_list == [
            call(0),
            call(1),
            call(2),
            call(4),
            call(5),
            call(3),
        ]


def test_drain() -> None:
    INTERVALS: list[tuple[int | str, ...]] = [
        (0, 1, 2, 3, 1),
        (2, 2, "This is an error."),
        (5, 3),
    ]
    threads = active_count()
    cb = MagicMock()
    with interleave(
        [sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS)],
        onerror=DRAIN,
    ) as it:
        for expected in [(0, 0), (0, 1), (1, 0), (0, 2), (1, 1), (2, 0), (0, 3)]:
            assert next(it) == expected
        with pytest.raises(RuntimeError) as excinfo:
            next(it)
        assert str(excinfo.value) == "This is an error."
        assert active_count() == threads
        assert cb.call_args_list == [call(1)]


def test_stop() -> None:
    INTERVALS: list[tuple[int | str, ...]] = [
        (0, 1, 2, 3, 1),
        (2, 2, "This is an error."),
        (5, 3),
    ]
    threads = active_count()
    cb = MagicMock()
    with interleave(
        [sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS)],
        onerror=STOP,
    ) as it:
        for expected in [(0, 0), (0, 1), (1, 0), (0, 2), (1, 1)]:
            assert next(it) == expected
        with pytest.raises(RuntimeError) as excinfo:
            next(it)
        assert str(excinfo.value) == "This is an error."
        assert active_count() == threads
        assert cb.call_args_list == [call(1)]


def test_with() -> None:
    INTERVALS = [
        (0, 1, 2),
        (2, 2, 2),
        (5, 2, 1),
    ]
    threads = active_count()
    cb = MagicMock()
    it = interleave(sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS))
    with it:
        assert list(it) == [
            (0, 0),
            (0, 1),
            (1, 0),
            (0, 2),
            (1, 1),
            (2, 0),
            (1, 2),
            (2, 1),
            (2, 2),
        ]
    assert active_count() == threads
    assert cb.call_args_list == [call(0), call(1), call(2)]


def test_with_early_break() -> None:
    INTERVALS = [
        (0, 1, 2),
        (2, 2, 2),
        (5, 2, 1),
    ]
    threads = active_count()
    cb = MagicMock()
    it = interleave(sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS))
    with it:
        for expected in [(0, 0), (0, 1), (1, 0), (0, 2), (1, 1), (2, 0), (1, 2)]:
            assert next(it) == expected
    assert active_count() == threads
    assert cb.call_args_list == [call(0), call(1)]


def test_extra_next() -> None:
    INTERVALS = [
        (0, 2),
        (1, 2),
    ]
    threads = active_count()
    it = interleave(sleeper(i, intervals) for i, intervals in enumerate(INTERVALS))
    with it:
        for expected in [(0, 0), (1, 0), (0, 1), (1, 1)]:
            assert next(it) == expected
        with pytest.raises(StopIteration):
            next(it)
        with pytest.raises(StopIteration):
            next(it)
        assert active_count() == threads


def test_shutdown_after_exhaustion() -> None:
    INTERVALS = [
        (0, 2),
        (1, 2),
    ]
    threads = active_count()
    with interleave(
        sleeper(i, intervals) for i, intervals in enumerate(INTERVALS)
    ) as it:
        for expected in [(0, 0), (1, 0), (0, 1), (1, 1)]:
            assert next(it) == expected
        with pytest.raises(StopIteration):
            next(it)
        assert active_count() == threads
        it.shutdown()
        with pytest.raises(StopIteration):
            next(it)
        assert active_count() == threads


def test_shutdown_after_error() -> None:
    INTERVALS: list[tuple[int | str, ...]] = [
        (0, 1, 2, 3, 1),
        (2, 2, "This is an error."),
        (5, 3),
    ]
    threads = active_count()
    with interleave(
        sleeper(i, intervals) for i, intervals in enumerate(INTERVALS)
    ) as it:
        for expected in [(0, 0), (0, 1), (1, 0), (0, 2), (1, 1)]:
            assert next(it) == expected
        with pytest.raises(RuntimeError) as excinfo:
            next(it)
        assert str(excinfo.value) == "This is an error."
        assert active_count() == threads
        it.shutdown()
        with pytest.raises(StopIteration):
            next(it)
        assert active_count() == threads


def test_shutdown_and_continue() -> None:
    INTERVALS = [
        (0, 1, 2),
        (2, 2, 2),
        (5, 2, 1),
    ]
    threads = active_count()
    cb = MagicMock()
    with interleave(
        sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS)
    ) as it:
        for expected in [(0, 0), (0, 1), (1, 0), (0, 2), (1, 1)]:
            assert next(it) == expected
        it.shutdown()
        assert active_count() == threads
        assert list(it) == [(2, 0), (1, 2)]
        assert cb.call_args_list == [call(0)]


def test_shutdown_while_pending() -> None:
    INTERVALS = [
        (0, 1, 2, 3),
        (2, 2, 3, 3),
        (5, 3, 3),
        (9, 3),
        (1, 1, 1),
    ]
    threads = active_count()
    cb = MagicMock()
    with interleave(
        [sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS)],
        max_workers=4,
    ) as it:
        for expected in [(0, 0), (0, 1), (1, 0), (0, 2), (1, 1)]:
            assert next(it) == expected
        it.shutdown()
        assert active_count() == threads
        assert list(it) == [(2, 0), (0, 3), (1, 2), (3, 0)]
        assert cb.call_args_list == []


def test_shutdown_in_with() -> None:
    INTERVALS = [
        (0, 1, 2),
        (2, 2, 2),
        (5, 2, 1),
    ]
    threads = active_count()
    cb = MagicMock()
    it = interleave(sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS))
    with it:
        for expected in [(0, 0), (0, 1), (1, 0), (0, 2), (1, 1)]:
            assert next(it) == expected
        it.shutdown()
        assert active_count() == threads
    assert active_count() == threads
    assert cb.call_args_list == [call(0)]


@pytest.mark.skipif(os.name != "posix", reason="POSIX only")
def test_ctrl_c() -> None:
    SCRIPT = Path(__file__).with_name("data") / "script.py"
    with Popen(
        [sys.executable, "-u", str(SCRIPT)],
        stdout=PIPE,
        universal_newlines=True,
        bufsize=1,
    ) as p:
        assert p.stdout is not None
        for expected in [(0, 0), (0, 1), (1, 0), (0, 2), (1, 1)]:
            assert p.stdout.readline() == f"{expected}\n"
        p.send_signal(SIGINT)
        r = p.wait(3 * UNIT)
        assert p.stdout.read() == ""
    if sys.version_info[:2] >= (3, 8):
        # For some reason, the script exits with rc 1 instead of -SIGINT on
        # Python 3.7.
        assert r == -SIGINT


def test_get_stop() -> None:
    INTERVALS: list[tuple[int | str, ...]] = [
        (0, 1, 2),
        (2, 2, "This is an error.", "This is not raised."),
        (5, "This is not seen.", 1),
    ]
    threads = active_count()
    cb = MagicMock()
    with interleave(
        sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS)
    ) as it:
        assert it.get() == (0, 0)
        with pytest.raises(Empty):
            it.get(block=False)
        t0 = monotonic()
        with pytest.raises(Empty):
            it.get(timeout=0.4 * UNIT)
        t1 = monotonic()
        assert isclose(t1 - t0, 0.4 * UNIT, rel_tol=0.3, abs_tol=0.1)
        assert it.get() == (0, 1)
        t2 = monotonic()
        assert isclose(t2 - t1, 0.6 * UNIT, rel_tol=0.3, abs_tol=0.1)
        with pytest.raises(Empty):
            it.get(block=False)
        sleep(UNIT * 1.2)
        assert it.get(block=False) == (1, 0)
        for expected in [(0, 2), (1, 1)]:
            assert it.get() == expected
        with pytest.raises(RuntimeError) as excinfo:
            it.get()
        with pytest.raises(EndOfInputError):
            it.get(block=False)
        with pytest.raises(EndOfInputError):
            it.get(block=False)
        assert str(excinfo.value) == "This is an error."
        assert active_count() == threads
        assert cb.call_args_list == [call(0), call(1)]


def test_get_finish_all() -> None:
    INTERVALS: list[tuple[int | str, ...]] = [
        (0, 1, 2, 3),
        (2, 2, "This is an error."),
        (5, "This error will be swallowed."),
    ]
    threads = active_count()
    cb = MagicMock()
    with interleave(
        [sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS)],
        onerror=FINISH_ALL,
    ) as it:
        assert it.get() == (0, 0)
        with pytest.raises(Empty):
            it.get(block=False)
        t0 = monotonic()
        with pytest.raises(Empty):
            it.get(timeout=0.4 * UNIT)
        t1 = monotonic()
        assert isclose(t1 - t0, 0.4 * UNIT, rel_tol=0.3, abs_tol=0.1)
        assert it.get() == (0, 1)
        t2 = monotonic()
        assert isclose(t2 - t1, 0.6 * UNIT, rel_tol=0.3, abs_tol=0.1)
        with pytest.raises(Empty):
            it.get(block=False)
        sleep(UNIT * 1.2)
        assert it.get(block=False) == (1, 0)
        for expected in [(0, 2), (1, 1)]:
            assert it.get() == expected
        sleep(UNIT * 0.7)
        with pytest.raises(Empty):
            it.get(block=False)
        t0 = monotonic()
        assert it.get(timeout=UNIT) == (2, 0)
        t1 = monotonic()
        assert isclose(t1 - t0, 0.3 * UNIT, rel_tol=0.3, abs_tol=0.1)
        sleep(UNIT * 0.2)
        t0 = monotonic()
        assert it.get(timeout=UNIT) == (0, 3)
        t1 = monotonic()
        assert isclose(t1 - t0, 0.8 * UNIT, rel_tol=0.3, abs_tol=0.1)
        with pytest.raises(RuntimeError) as excinfo:
            it.get()
        with pytest.raises(EndOfInputError):
            it.get(block=False)
        with pytest.raises(EndOfInputError):
            it.get(block=False)
        assert str(excinfo.value) == "This is an error."
        assert active_count() == threads


def test_submit() -> None:
    threads = active_count()
    it: Interleaver[tuple[int, int]] = Interleaver()
    with it:
        assert active_count() == threads
        with pytest.raises(Empty):
            it.get(timeout=UNIT)
        it.submit(sleeper(0, (0, 3)))
        it.submit(sleeper(1, (1, 3)))
        assert it.get(timeout=UNIT) == (0, 0)
        assert it.get() == (1, 0)
        it.submit(sleeper(2, (1, 3)))
        assert it.get() == (2, 0)
        assert it.get() == (0, 1)
        assert it.get() == (1, 1)
        assert it.get() == (2, 1)
        with pytest.raises(Empty):
            it.get(timeout=UNIT)
        it.submit(sleeper(3, (0, 1, 1)))
        it.finalize()
        with pytest.raises(ValueError) as excinfo:
            it.submit(sleeper(4, (0, 1, 1)))
        assert (
            str(excinfo.value)
            == "Cannot submit new producers after finalize() is called"
        )
        assert it.get() == (3, 0)
        assert it.get() == (3, 1)
        assert it.get() == (3, 2)
        with pytest.raises(EndOfInputError):
            it.get()
        assert active_count() == threads


def test_submit_after_shutdown() -> None:
    threads = active_count()
    it: Interleaver[tuple[int, int]] = Interleaver()
    with it:
        assert active_count() == threads
        it.submit(sleeper(0, (0, 1, 1)))
        assert it.get() == (0, 0)
        it.shutdown()
        assert active_count() == threads
        with pytest.raises(ValueError) as excinfo:
            it.submit(sleeper(1, (0, 1, 1)))
        assert (
            str(excinfo.value)
            == "Cannot submit new producers after finalize() is called"
        )
        assert active_count() == threads


def test_finalize_on_shutdown() -> None:
    threads = active_count()
    it: Interleaver[tuple[int, int]] = Interleaver()
    with it:
        assert active_count() == threads
        with pytest.raises(Empty):
            it.get(timeout=UNIT)
        it.submit(sleeper(0, (0, 3)))
        it.submit(sleeper(1, (1, 3)))
        assert it.get(timeout=UNIT) == (0, 0)
        assert it.get() == (1, 0)
        assert it.get() == (0, 1)
        assert it.get() == (1, 1)
        with pytest.raises(Empty):
            it.get(timeout=UNIT)
        it.shutdown()
        with pytest.raises(EndOfInputError):
            it.get()
        assert active_count() == threads


@pytest.mark.parametrize("onerror", list(OnError))
def test_error_no_internals_in_traceback(onerror: OnError) -> None:
    INTERVALS: list[tuple[int | str, ...]] = [
        (0, 1, 2),
        (2, 2, "This is an error.", "This is not raised."),
        (5, "This is not seen.", 1),
    ]
    with interleave(
        [sleeper(i, intervals) for i, intervals in enumerate(INTERVALS)],
        onerror=onerror,
    ) as it:
        with pytest.raises(RuntimeError) as excinfo:
            list(it)
        assert "EndOfInputError" not in "".join(
            traceback.format_exception(excinfo.type, excinfo.value, excinfo.tb)
        )
