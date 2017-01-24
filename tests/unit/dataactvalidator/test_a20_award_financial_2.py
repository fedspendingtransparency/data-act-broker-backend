from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'a20_award_financial_2'


def test_column_headers(database):
    expected_subset = {"row_number", "agency_identifier"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test that agency codes are matched against cgac correctly """
    award_fin = AwardFinancialFactory()
    award_fin_null = AwardFinancialFactory(agency_identifier=None)
    cgac = CGACFactory(cgac_code=award_fin.agency_identifier)

    errors = number_of_errors(_FILE, database, models=[award_fin, award_fin_null, cgac])
    assert errors == 0


def test_failure(database):
    """ Test a cgac not present in cgac table"""
    # These cgacs are different lengths to avoid being equal
    cgac_one = "cgac_one"
    cgac_two = "cgac_two"
    award_fin = AwardFinancialFactory(agency_identifier=cgac_one)
    cgac = CGACFactory(cgac_code=cgac_two)

    errors = number_of_errors(_FILE, database, models=[award_fin, cgac])
    assert errors == 1
