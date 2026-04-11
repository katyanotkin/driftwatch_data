from driftwatch.yf_client import _safe_float, _safe_int


def test_safe_float_normal():
    assert _safe_float(1.5) == 1.5

def test_safe_float_int_input():
    assert _safe_float(42) == 42.0

def test_safe_float_string():
    assert _safe_float("3.14") == 3.14

def test_safe_float_none():
    assert _safe_float(None) is None

def test_safe_float_nan():
    assert _safe_float(float("nan")) is None

def test_safe_float_invalid_string():
    assert _safe_float("abc") is None

def test_safe_int_normal():
    assert _safe_int(3) == 3

def test_safe_int_float_input():
    assert _safe_int(3.9) == 3  # truncates, not rounds

def test_safe_int_none():
    assert _safe_int(None) is None

def test_safe_int_invalid():
    assert _safe_int("abc") is None
