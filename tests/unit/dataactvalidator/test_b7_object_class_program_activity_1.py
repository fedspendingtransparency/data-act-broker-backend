from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b7_object_class_program_activity_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'gross_outlays_delivered_or_fyb', 'ussgl490800_authority_outl_fyb'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test Object Class Program Activity gross_outlays_delivered_or_fyb equals ussgl490800_authority_outl_fyb """

    op = ObjectClassProgramActivityFactory(gross_outlays_delivered_or_fyb=1, ussgl490800_authority_outl_fyb=1)

    assert number_of_errors(_FILE, database, models=[op]) == 0


def test_failure(database):
    """ Test Object Class Program Activity gross_outlays_delivered_or_fyb doesnt' equals
    ussgl490800_authority_outl_fyb """

    op = ObjectClassProgramActivityFactory(gross_outlays_delivered_or_fyb=1, ussgl490800_authority_outl_fyb=0)

    assert number_of_errors(_FILE, database, models=[op]) == 1

