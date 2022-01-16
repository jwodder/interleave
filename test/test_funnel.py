from queue import Empty
import pytest
from interleave import EndOfInputError, FunnelQueue


def test_funnel() -> None:
    funnel: FunnelQueue[int] = FunnelQueue()
    ctx1 = funnel.putting()
    ctx2 = funnel.putting()
    funnel.finalize()
    with ctx1:
        funnel.put(1)
    assert funnel.get() == 1
    with pytest.raises(Empty):
        funnel.get(block=False)
    with ctx2:
        pass
    with pytest.raises(EndOfInputError):
        funnel.get()
    with pytest.raises(EndOfInputError):
        funnel.get()


def test_funnel_put_after_end() -> None:
    funnel: FunnelQueue[int] = FunnelQueue()
    ctx1 = funnel.putting()
    funnel.finalize()
    with ctx1:
        funnel.put(1)
    assert funnel.get() == 1
    with pytest.raises(EndOfInputError):
        funnel.get()
    with pytest.raises(ValueError):
        funnel.put(2)
    with pytest.raises(EndOfInputError):
        funnel.get()


def test_funnel_submit_after_finalize() -> None:
    funnel: FunnelQueue[int] = FunnelQueue()
    funnel.putting()
    funnel.putting()
    funnel.finalize()
    with pytest.raises(ValueError) as excinfo:
        funnel.putting()
    assert (
        str(excinfo.value) == "Cannot submit new producers after finalize() is called"
    )


def test_funnel_no_producers() -> None:
    funnel: FunnelQueue[int] = FunnelQueue()
    with pytest.raises(Empty):
        funnel.get(block=False)
    funnel.finalize()
    with pytest.raises(EndOfInputError):
        funnel.get()
