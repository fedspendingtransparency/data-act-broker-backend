from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'a21_appropriations'

def test_column_headers(database):
    expected_subset = {"row_number", "availability_type_code"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual

def test_success(database):
    """ Test that availability type code is either x or absent """
    approp = AppropriationFactory(availability_type_code = 'X')
    approp_lower = AppropriationFactory(availability_type_code = 'x')
    approp_null = AppropriationFactory(availability_type_code = None)

    errors = number_of_errors(_FILE, database, models=[approp, approp_lower, approp_null])
    assert errors == 0

def test_failure(database):
    """ Test an incorrect availability_type_code """
    approp = AppropriationFactory(availability_type_code = 'z')

    errors = number_of_errors(_FILE, database, models=[approp])
    assert errors == 1