import pytest

from dataactbroker.helpers.generic_helper import year_period_to_dates

from dataactcore.utils.responseException import ResponseException


def test_year_period_to_dates():
    """ Test successful conversions from quarter to dates """
    # Test year/period that has dates in the same year
    start, end = year_period_to_dates(2017, 4)
    assert start == '01/01/2017'
    assert end == '01/31/2017'

    # Test year/period that has dates in the previous year
    start, end = year_period_to_dates(2017, 2)
    assert start == '11/01/2016'
    assert end == '11/30/2016'


def test_year_period_to_dates_period_failure():
    """ Test invalid quarter formats """
    error_text = 'Period must be an integer 2-12.'

    # Test period that's too high
    with pytest.raises(ResponseException) as resp_except:
        year_period_to_dates(2017, 13)

    assert resp_except.value.status == 400
    assert str(resp_except.value) == error_text

    # Test period that's too low
    with pytest.raises(ResponseException) as resp_except:
        year_period_to_dates(2017, 1)

    assert resp_except.value.status == 400
    assert str(resp_except.value) == error_text

    # Test null period
    with pytest.raises(ResponseException) as resp_except:
        year_period_to_dates(2017, None)

    assert resp_except.value.status == 400
    assert str(resp_except.value) == error_text


def test_year_period_to_dates_year_failure():
    error_text = 'Year must be in YYYY format.'
    # Test null year
    with pytest.raises(ResponseException) as resp_except:
        year_period_to_dates(None, 2)

    assert resp_except.value.status == 400
    assert str(resp_except.value) == error_text

    # Test invalid year
    with pytest.raises(ResponseException) as resp_except:
        year_period_to_dates(999, 2)

    assert resp_except.value.status == 400
    assert str(resp_except.value) == error_text
