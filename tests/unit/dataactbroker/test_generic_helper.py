import pytest
import datetime as dt
from sqlalchemy import func, or_

from dataactbroker.helpers.generic_helper import year_period_to_dates, generate_raw_quoted_query, fy
from dataactcore.models.jobModels import FileGeneration

from dataactcore.utils.responseException import ResponseException

legal_dates = {
    dt.datetime(2017, 2, 2, 16, 43, 28, 377373): 2017,
    dt.date(2017, 2, 2): 2017,
    dt.datetime(2017, 10, 2, 16, 43, 28, 377373): 2018,
    dt.date(2017, 10, 2): 2018,
    '1000-09-30': 1000,
    '1000-10-01': 1001,
    '09-30-2000': 2000,
    '10-01-2000': 2001,
    '10-01-01': 2002
}

not_dates = (0, 2017.2, 'forthwith', 'string', '')


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


def test_generate_raw_quoted_query(database):
    sess = database.session
    # Using FileGeneration for example

    # Testing various filter logic
    q = sess.query(FileGeneration.created_at).filter(
        or_(FileGeneration.file_generation_id == 1, FileGeneration.request_date > datetime.datetime(2018, 1, 15, 0, 0)),
        FileGeneration.agency_code.like('A'),
        FileGeneration.file_path.is_(None),
        FileGeneration.agency_type.in_(['awarding', 'funding']),
        FileGeneration.agency_type.in_([('test',)]),
        FileGeneration.is_cached_file.is_(True)
    )
    expected = "SELECT file_generation.created_at  " \
               "FROM file_generation  " \
               "WHERE " \
               "(file_generation.file_generation_id = 1 OR file_generation.request_date > '2018-01-15 00:00:00') " \
               "AND file_generation.agency_code LIKE 'A' " \
               "AND file_generation.file_path IS NULL " \
               "AND file_generation.agency_type IN ('awarding', 'funding') " \
               "AND file_generation.agency_type IN ('(''test'',)') " \
               "AND file_generation.is_cached_file IS true"
    assert generate_raw_quoted_query(q) == expected

    # Testing funcs
    q = sess.query(func.max(FileGeneration.file_generation_id).label("Test Label"))
    expected = 'SELECT max(file_generation.file_generation_id) AS "Test Label"  ' \
               'FROM file_generation'
    assert generate_raw_quoted_query(q) == expected

@pytest.mark.parametrize("raw_date, expected_fy", legal_dates.items())
def test_fy_returns_integer(raw_date, expected_fy):
    assert isinstance(fy(raw_date), int)


@pytest.mark.parametrize("raw_date, expected_fy", legal_dates.items())
def test_fy_returns_correct(raw_date, expected_fy):
    assert fy(raw_date) == expected_fy


@pytest.mark.parametrize("not_date", not_dates)
def test_fy_type_exceptions(not_date):
    assert fy(None) == None

    with pytest.raises(TypeError):
        fy(not_date)
