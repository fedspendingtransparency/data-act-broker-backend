from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b11_object_class_program_activity_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'object_class', 'by_direct_reimbursable_fun'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test valid 4 digit object class code and corresponding reimbursable funding source """

    op = ObjectClassProgramActivityFactory(object_class='123', by_direct_reimbursable_fun='X')
    assert number_of_errors(_FILE, database, models=[op]) == 0

    op = ObjectClassProgramActivityFactory(object_class='1234', by_direct_reimbursable_fun='D')
    assert number_of_errors(_FILE, database, models=[op]) == 0

    op = ObjectClassProgramActivityFactory(object_class='2234', by_direct_reimbursable_fun='r')
    assert number_of_errors(_FILE, database, models=[op]) == 0


def test_failure(database):
    """ Test invalid 4 digit object class code and corresponding reimbursable funding source """

    op = ObjectClassProgramActivityFactory(object_class='1234', by_direct_reimbursable_fun='x')
    assert number_of_errors(_FILE, database, models=[op]) == 1

    op = ObjectClassProgramActivityFactory(object_class='2234', by_direct_reimbursable_fun='Y')
    assert number_of_errors(_FILE, database, models=[op]) == 1