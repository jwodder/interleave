from math import isclose
import os
from threading import active_count
from time import monotonic, sleep
from typing import Any, Callable, Iterator, List, Optional, Sequence, Tuple, Union
from unittest.mock import MagicMock, call
import pytest
from interleave import interleave

UNIT = 0.5


def sleeper(
    tid: int,
    delays: Sequence[Union[int, str]],
    done_callback: Optional[Callable[[int], Any]] = None,
) -> Iterator[Tuple[int, int]]:
    try:
        for i, d in enumerate(delays):
            if isinstance(d, int):
                sleep(d * UNIT)
                yield (tid, i)
            else:
                raise RuntimeError(d)
    finally:
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
    assert list(
        interleave(sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS))
    ) == [
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


@pytest.mark.flaky(reruns=5, condition="CI" in os.environ)
def test_simple_timing() -> None:
    INTERVALS = [
        (0, 1, 2),
        (2, 2, 2),
        (5, 2, 1),
    ]
    prev: Optional[float] = None
    for _ in interleave(sleeper(i, intervals) for i, intervals in enumerate(INTERVALS)):
        now = monotonic()
        if prev is not None:
            assert isclose(now - prev, UNIT, rel_tol=0.3, abs_tol=0.1)
        prev = now


def test_simple_error() -> None:
    INTERVALS: List[Tuple[Union[int, str], ...]] = [
        (0, 1, 2),
        (2, 2, "This is an error.", "This is not raised."),
        (5, "This is not seen.", 1),
    ]
    threads = active_count()
    cb = MagicMock()
    it = interleave(sleeper(i, intervals, cb) for i, intervals in enumerate(INTERVALS))
    for expected in [(0, 0), (0, 1), (1, 0), (0, 2), (1, 1)]:
        assert next(it) == expected
    with pytest.raises(RuntimeError) as excinfo:
        next(it)
    assert str(excinfo.value) == "This is an error."
    assert active_count() == threads
    assert cb.call_args_list == [call(0), call(1)]


def test_no_iterators() -> None:
    it: Iterator[Any] = interleave([])
    with pytest.raises(StopIteration):
        next(it)


def test_ragged() -> None:
    INTERVALS = [
        (0, 1, 2, 3, 3),
        (2, 2, 3),
        (5, 3),
    ]
    assert list(
        interleave(sleeper(i, intervals) for i, intervals in enumerate(INTERVALS))
    ) == [
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
