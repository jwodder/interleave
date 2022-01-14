from time import sleep
from typing import Iterator, Sequence, Tuple
from interleave import interleave

UNIT = 0.5


def sleeper(tid: int, delays: Sequence[int]) -> Iterator[Tuple[int, int]]:
    for i, d in enumerate(delays):
        sleep(d * UNIT)
        yield (tid, i)


def test_simple_ordering() -> None:
    INTERVALS = [
        (0, 1, 2),
        (2, 2, 2),
        (5, 2, 1),
    ]
    assert list(
        interleave([sleeper(i, intervals) for i, intervals in enumerate(INTERVALS)])
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
