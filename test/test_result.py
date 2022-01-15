from typing import Optional
import pytest
from interleave import Result


def test_successful() -> None:
    xs = [1, 2, 3]
    r = Result(xs)
    assert r.success
    assert r.get() is xs


def test_none_value() -> None:
    r: Result[Optional[int]] = Result(None)
    assert r.success
    assert r.get() is None


def test_reraising() -> None:
    try:
        raise ValueError("Something went wrong")
    except BaseException:
        r = Result.for_exc()
    assert not r.success
    with pytest.raises(ValueError) as excinfo:
        r.get()
    assert str(excinfo.value) == "Something went wrong"


def test_not_for_exc() -> None:
    with pytest.raises(ValueError) as excinfo:
        Result.for_exc()
    assert str(excinfo.value) == "No exception currently being handled"
