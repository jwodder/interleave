from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
from typing import Iterable, Iterator, Optional, TypeVar

T = TypeVar("T")


def interleave(
    iterators: Iterable[Iterator[T]],
    *,
    max_workers: Optional[int] = None,
    queue_size: int = 0,
    queue_wait: float = 1.0
) -> Iterator[T]:
    pool = ThreadPoolExecutor(max_workers=max_workers)
    queue = Queue(queue_size)
    futures = []

    def run_iterator(it: Iterator[T]) -> None:
        for x in it:
            queue.put(x)

    for it in iterators:
        futures.append(pool.submit(run_iterator, it))

    while True:
        try:
            # We need to set a timeout so that keyboard interrupts aren't
            # ignored on Windows
            x = queue.get(timeout=queue_wait)
        except Empty:
            if all(f.done() for f in futures):
                break
        else:
            yield x

    for f in futures:
        e = f.exception()
        if e is not None:
            raise e
