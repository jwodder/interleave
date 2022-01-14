import pytest
from interleave import Result


def test_reraising() -> None:
    try:
        raise ValueError("Something went wrong")
    except BaseException:
        r = Result.for_exc()
    with pytest.raises(ValueError) as excinfo:
        r.get()
    assert str(excinfo.value) == "Something went wrong"
