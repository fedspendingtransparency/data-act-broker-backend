from random import randint

from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c20_award_financial'
_TAS = 'c20_award_financial_tas'


def test_column_headers(database):
    expected_subset = {'row_number'}
    actual = set(query_columns(_FILE, database.stagingDb))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that the sum of financial elements in File C is less than or equal
     to the corresponding element in File B for the same TAS and Object Class combination"""
    submission_id = randint(1000, 10000)
    tas, object_class = 'some-tas', 'some-code'


def test_failure(database):
    """ Tests that the sum of financial elements in File C is not less than or equal
         to the corresponding element in File B for the same TAS and Object Class combination"""
    submission_id = randint(1000, 10000)
    tas, object_class = 'some-tas', 'some-code'