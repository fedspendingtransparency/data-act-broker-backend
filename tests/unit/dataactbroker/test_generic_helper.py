import pytest

from dataactbroker.helpers.generic_helper import quarter_to_dates

from dataactcore.utils.responseException import ResponseException


def test_quarter_to_dates():
    """ Test successful conversions from quarter to dates """
    # Test quarter that has dates in the same year
    start, end = quarter_to_dates('Q2/2017')
    assert start == '01/01/2017'
    assert end == '03/31/2017'

    # Test quarter that has dates in the previous year
    start, end = quarter_to_dates('Q1/2017')
    assert start == '10/01/2016'
    assert end == '12/31/2016'


def test_quarter_to_dates_failure():
    """ Test invalid quarter formats """
    error_text = 'Quarter must be in Q#/YYYY format, where # is 1-4.'

    # Test quarter that's too high
    with pytest.raises(ResponseException) as resp_except:
        quarter_to_dates('Q5/2017')

    assert resp_except.value.status == 400
    assert str(resp_except.value) == error_text

    # Test null quarter
    with pytest.raises(ResponseException) as resp_except:
        quarter_to_dates(None)

    assert resp_except.value.status == 400
    assert str(resp_except.value) == error_text

    # Test correct numbers but bad format
    with pytest.raises(ResponseException) as resp_except:
        quarter_to_dates('Q11/017')

    assert resp_except.value.status == 400
    assert str(resp_except.value) == error_text
