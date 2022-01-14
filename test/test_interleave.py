from math import isclose
from time import monotonic, sleep
from typing import Iterator, Sequence, Tuple
from interleave import interleave

UNIT = 0.5


def sleeper(tid: int, delays: Sequence[int]) -> Iterator[Tuple[int, int]]:
    for i, d in enumerate(delays):
        sleep(d * UNIT)
        yield (tid, i)


def test_simple() -> None:
    INTERVALS = [
        (0, 1, 2),
        (2, 2, 2),
        (5, 2, 1),
    ]
    woven = []
    start = monotonic()
    for i, x in enumerate(
        interleave([sleeper(i, intervals) for i, intervals in enumerate(INTERVALS)])
    ):
        assert isclose(monotonic() - start, i * UNIT, rel_tol=0.1, abs_tol=0.1)
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
