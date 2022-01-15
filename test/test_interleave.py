from math import isclose
from time import monotonic, sleep
from typing import Iterator, List, Optional, Sequence, Tuple, Union
import pytest
from interleave import interleave

UNIT = 0.5


def close2unit(f: float) -> bool:
    return isclose(f, UNIT, rel_tol=0.3, abs_tol=0.1)


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
    last_yield: Optional[float] = None
    for x in interleave(sleeper(i, intervals) for i, intervals in enumerate(INTERVALS)):
        this_yield = monotonic()
        if last_yield is not None:
            assert close2unit(this_yield - last_yield)
        last_yield = this_yield
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
    last_yield: Optional[float] = None
    i = 0
    it = interleave(sleeper(i, intervals) for i, intervals in enumerate(INTERVALS))
    while True:
        if i < len(expected):
            x = next(it)
            assert x == expected[i]
            this_yield = monotonic()
            if last_yield is not None:
                assert close2unit(this_yield - last_yield)
            last_yield = this_yield
            i += 1
        else:
            with pytest.raises(RuntimeError) as excinfo:
                next(it)
            assert str(excinfo.value) == "This is an error."
            break
