import os
from time import sleep
from interleave import interleave

UNIT = 1.0 if "CI" in os.environ else 0.25


def sleeper(idno, delays):
    for i, d in enumerate(delays):
        sleep(d * UNIT)
        yield (idno, i)


with interleave(
    [
        sleeper(0, [0, 1, 2]),
        sleeper(1, [2, 2, 2]),
        sleeper(2, [5, 2, 1]),
    ]
) as it:
    for x in it:
        print(x)
