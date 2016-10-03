from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from random import choice
from string import ascii_uppercase, ascii_lowercase, digits

_FILE = 'a20_object_class_program_activity_1'

def test_column_headers(database):
    expected_subset = {"row_number", "allocation_transfer_agency"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset <= actual

def test_success(database):
    """ Test that TAS values can be found, and null matches work correctly"""
    ocpa = ObjectClassProgramActivityFactory()
    cgac = CGACFactory(cgac_code = ocpa.allocation_transfer_agency)

    errors = number_of_errors(_FILE, database, models=[ocpa, cgac])
    assert errors == 0

def test_failure(database):
    """ Test that tas that does not match is an error"""
    # These cgacs are different lengths to avoid being equal
    cgac_one = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(5))
    cgac_two = ''.join(choice(ascii_uppercase + ascii_lowercase + digits) for i in range(4))
    ocpa = ObjectClassProgramActivityFactory(allocation_transfer_agency = cgac_one)
    cgac = CGACFactory(cgac_code = cgac_two)

    errors = number_of_errors(_FILE, database, models=[ocpa, cgac])
    assert errors == 2