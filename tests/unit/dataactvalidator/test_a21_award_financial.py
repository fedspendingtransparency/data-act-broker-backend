from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'a21_award_financial'

def test_column_headers(database):
    expected_subset = {"row_number", "availability_type_code"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual

def test_success(database):
    """ Test that availability type code is either x or absent """
    award_fin = AwardFinancialFactory(availability_type_code = 'X')
    award_fin_lower = AwardFinancialFactory(availability_type_code = 'x')
    award_fin_null = AwardFinancialFactory(availability_type_code = None)

    errors = number_of_errors(_FILE, database, models=[award_fin, award_fin_null, award_fin_lower])
    assert errors == 0

def test_failure(database):
    """ Test an incorrect availability_type_code """
    award_fin = AwardFinancialFactory(availability_type_code = 'z')

    errors = number_of_errors(_FILE, database, models=[award_fin])
    assert errors == 1