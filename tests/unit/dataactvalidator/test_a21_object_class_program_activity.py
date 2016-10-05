from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'a21_object_class_program_activity'

def test_column_headers(database):
    expected_subset = {"row_number", "availability_type_code"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual

def test_success(database):
    """ Test that availability type code is either x or absent """
    ocpa = ObjectClassProgramActivityFactory(availability_type_code = 'X')
    ocpa_lower = ObjectClassProgramActivityFactory(availability_type_code = 'x')
    ocpa_null = ObjectClassProgramActivityFactory(availability_type_code = None)

    errors = number_of_errors(_FILE, database, models=[ocpa, ocpa_null, ocpa_lower])
    assert errors == 0

def test_failure(database):
    """ Test an incorrect availability_type_code """
    ocpa = ObjectClassProgramActivityFactory(availability_type_code = 'z')

    errors = number_of_errors(_FILE, database, models=[ocpa])
    assert errors == 1