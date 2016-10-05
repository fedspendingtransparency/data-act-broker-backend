from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from random import choice
from string import ascii_uppercase, ascii_lowercase, digits

_FILE = 'a20_award_financial_1'

def test_column_headers(database):
    expected_subset = {"row_number", "allocation_transfer_agency"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual

def test_success(database):
    """ Test that agency codes are matched against cgac correctly """
    award_fin = AwardFinancialFactory()
    award_fin_null = AwardFinancialFactory(allocation_transfer_agency = None)
    cgac = CGACFactory(cgac_code = award_fin.allocation_transfer_agency)

    errors = number_of_errors(_FILE, database, models=[award_fin, award_fin_null, cgac])
    assert errors == 0

def test_failure(database):
    """ Test a cgac not present in cgac table """
    # These cgacs are different lengths to avoid being equal
    cgac_one = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(5))
    cgac_two = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(4))
    awardFin = AwardFinancialFactory(allocation_transfer_agency = cgac_one)
    cgac = CGACFactory(cgac_code = cgac_two)

    errors = number_of_errors(_FILE, database, models=[awardFin, cgac])
    assert errors == 1