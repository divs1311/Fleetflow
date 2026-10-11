import pytest

from porterflow.quality.checks import _status


@pytest.mark.parametrize("value, expected", [(0.01, "PASS"), (0.04, "WARN"), (0.2, "FAIL")])
def test_upper_thresholds(value, expected):
    assert _status(value, warn_above=0.03, fail_above=0.10) == expected


@pytest.mark.parametrize("value, expected", [(1.0, "PASS"), (0.5, "WARN"), (0.2, "FAIL"), (3.0, "FAIL")])
def test_band_thresholds(value, expected):
    assert _status(value, warn_below=0.6, fail_below=0.3, warn_above=1.5, fail_above=2.5) == expected
