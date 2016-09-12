from random import randint

from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a33_appropriations_1'
_TAS = 'a33_appropriations_tas'


def test_column_headers(database):
    expected_subset = {'row_number'}
    actual = set(query_columns(_FILE, database.stagingDb))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that the sum of financial elements in File C is less than or equal
     to the corresponding element in File B for the same TAS and Program Activity
     Code combination"""
    submission_id = randint(1000, 10000)
    tas, code = 'some-tas', 'some-code'


def test_failure(database):
    """ Tests that the sum of financial elements in File C is not less than or equal
     to the corresponding element in File B for the same TAS and Program Activity
     Code combination"""
    submission_id = randint(1000, 10000)
    tas, code = 'some-tas', 'some-code'