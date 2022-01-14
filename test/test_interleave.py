from math import isclose
from time import monotonic, sleep
from typing import Iterator, List, Sequence, Tuple, Union
import pytest
from interleave import interleave

UNIT = 0.5


def sleeper(tid: int, delays: Sequence[Union[int, str]]) -> Iterator[Tuple[int, int]]:
    for i, d in enumerate(delays):
        if isinstance(d, int):
            sleep(d * UNIT)
            yield (tid, i)
        else:
            raise RuntimeError(d)


def test_simple() -> None:
    INTERVALS = [
        (0, 1, 2),
        (2, 2, 2),
        (5, 2, 1),
    ]
    woven = []
    start = monotonic()
    for i, x in enumerate(
        interleave(sleeper(i, intervals) for i, intervals in enumerate(INTERVALS))
    ):
        assert isclose(monotonic() - start, i * UNIT, rel_tol=0.3, abs_tol=0.1)
        woven.append(x)
    assert woven == [
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


def test_simple_error() -> None:
    INTERVALS: List[Tuple[Union[int, str], ...]] = [
        (0, 1, 2),
        (2, 2, "This is an error.", "This is not raised."),
        (5, "This is not seen.", 1),
    ]
    expected = [
        (0, 0),
        (0, 1),
        (1, 0),
        (0, 2),
        (1, 1),
    ]
    start = monotonic()
    i = 0
    it = interleave(sleeper(i, intervals) for i, intervals in enumerate(INTERVALS))
    while True:
        if i < len(expected):
            x = next(it)
            assert x == expected[i]
            assert isclose(monotonic() - start, i * UNIT, rel_tol=0.1, abs_tol=0.1)
            i += 1
        else:
            with pytest.raises(RuntimeError) as excinfo:
                next(it)
            assert str(excinfo.value) == "This is an error."
            break
