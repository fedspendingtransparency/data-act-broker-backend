import pytest
import datetime as dt
import os
import shutil
from filecmp import dircmp
from zipfile import ZipFile
from sqlalchemy import func, or_

from dataactbroker.helpers.generic_helper import (
    year_period_to_dates,
    generate_raw_quoted_query,
    fy,
    batch as batcher,
    zip_dir,
)
from dataactcore.models.jobModels import FileGeneration

from dataactcore.utils.ResponseError import ResponseError

legal_dates = {
    dt.datetime(2017, 2, 2, 16, 43, 28, 377373): 2017,
    dt.date(2017, 2, 2): 2017,
    dt.datetime(2017, 10, 2, 16, 43, 28, 377373): 2018,
    dt.date(2017, 10, 2): 2018,
    "1000-09-30": 1000,
    "1000-10-01": 1001,
    "09-30-2000": 2000,
    "10-01-2000": 2001,
    "10-01-01": 2002,
}

not_dates = (0, 2017.2, "forthwith", "string", "")


def test_year_period_to_dates():
    """Test successful conversions from quarter to dates"""
    # Test year/period that has dates in the same year
    start, end = year_period_to_dates(2017, 4)
    assert start == "01/01/2017"
    assert end == "01/31/2017"

    # Test year/period that has dates in the previous year
    start, end = year_period_to_dates(2017, 2)
    assert start == "11/01/2016"
    assert end == "11/30/2016"


def test_year_period_to_dates_period_failure():
    """Test invalid quarter formats"""
    error_text = "Period must be an integer 2-12."

    # Test period that's too high
    with pytest.raises(ResponseError) as resp_except:
        year_period_to_dates(2017, 13)

    assert resp_except.value.status == 400
    assert str(resp_except.value) == error_text

    # Test period that's too low
    with pytest.raises(ResponseError) as resp_except:
        year_period_to_dates(2017, 1)

    assert resp_except.value.status == 400
    assert str(resp_except.value) == error_text

    # Test null period
    with pytest.raises(ResponseError) as resp_except:
        year_period_to_dates(2017, None)

    assert resp_except.value.status == 400
    assert str(resp_except.value) == error_text


def test_year_period_to_dates_year_failure():
    error_text = "Year must be in YYYY format."
    # Test null year
    with pytest.raises(ResponseError) as resp_except:
        year_period_to_dates(None, 2)

    assert resp_except.value.status == 400
    assert str(resp_except.value) == error_text

    # Test invalid year
    with pytest.raises(ResponseError) as resp_except:
        year_period_to_dates(999, 2)

    assert resp_except.value.status == 400
    assert str(resp_except.value) == error_text


def test_generate_raw_quoted_query(database):
    sess = database.session
    # Using FileGeneration for example

    # Testing various filter logic
    q = sess.query(FileGeneration.created_at).filter(
        or_(FileGeneration.file_generation_id == 1, FileGeneration.request_date > dt.datetime(2018, 1, 15, 0, 0)),
        FileGeneration.agency_code.like("A"),
        FileGeneration.file_path.is_(None),
        FileGeneration.agency_type.in_(["awarding", "funding"]),
        FileGeneration.agency_type.in_([("test",)]),
        FileGeneration.is_cached_file.is_(True),
    )
    expected = (
        "SELECT file_generation.created_at  "
        "FROM file_generation  "
        "WHERE "
        "(file_generation.file_generation_id = 1 OR file_generation.request_date > '2018-01-15 00:00:00') "
        "AND file_generation.agency_code LIKE 'A' "
        "AND file_generation.file_path IS NULL "
        "AND file_generation.agency_type IN ('awarding', 'funding') "
        "AND file_generation.agency_type IN ('(''test'',)') "
        "AND file_generation.is_cached_file IS true"
    )
    assert generate_raw_quoted_query(q) == expected

    # Testing funcs
    q = sess.query(func.max(FileGeneration.file_generation_id).label("Test Label"))
    expected = 'SELECT max(file_generation.file_generation_id) AS "Test Label"  ' "FROM file_generation"
    assert generate_raw_quoted_query(q) == expected


@pytest.mark.parametrize("raw_date, expected_fy", legal_dates.items())
def test_fy_returns_integer(raw_date, expected_fy):
    assert isinstance(fy(raw_date), int)


@pytest.mark.parametrize("raw_date, expected_fy", legal_dates.items())
def test_fy_returns_correct(raw_date, expected_fy):
    assert fy(raw_date) == expected_fy


@pytest.mark.parametrize("not_date", not_dates)
def test_fy_type_exceptions(not_date):
    assert fy(None) is None

    with pytest.raises(TypeError):
        fy(not_date)


def test_batch():
    """Testing the batch function into chunks of 100"""
    full_list = list(range(0, 1000))
    initial_batch = list(range(0, 100))
    iteration = 0
    batch_size = 100
    for batch in batcher(full_list, batch_size):
        expected_batch = [x + (batch_size * iteration) for x in initial_batch]
        assert expected_batch == batch
        iteration += 1
    assert iteration == 10


def test_zip_dir():
    """Testing creating a zip with the zip_dir function"""
    # make a directory with a couple files
    test_dir_path = "test directory"
    os.mkdir(test_dir_path)
    test_files = {
        "test file a.txt": "TEST",
        "test file b.txt": "FILES",
        "test file c.txt": "abcd",
    }
    for test_file_path, test_file_content in test_files.items():
        with open(os.path.join(test_dir_path, test_file_path), "w") as test_file:
            test_file.write(test_file_content)

    # zip it
    test_zip_path = zip_dir(test_dir_path, "test zip")

    # keep the original directory and files to compare
    os.rename(test_dir_path, "{} original".format(test_dir_path))

    assert test_zip_path == os.path.abspath("test zip.zip")

    # confirm zip inside has the files
    ZipFile(test_zip_path).extractall()
    assert os.path.exists(test_dir_path)
    dir_comp = dircmp("{} original".format(test_dir_path), test_dir_path)
    assert dir_comp.left_only == []
    assert dir_comp.right_only == []
    assert dir_comp.diff_files == []

    # cleanup
    os.remove(test_zip_path)
    shutil.rmtree(test_dir_path)
    shutil.rmtree("{} original".format(test_dir_path))
