from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b6_object_class_program_activity_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'gross_outlays_undelivered_fyb', 'ussgl480200_undelivered_or_fyb'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test Object Class Program Activity gross_outlays_undelivered_fyb equals ussgl480200_undelivered_or_fyb """

    op = ObjectClassProgramActivityFactory(gross_outlays_undelivered_fyb=1, ussgl480200_undelivered_or_fyb=1)

    assert number_of_errors(_FILE, database, models=[op]) == 0


def test_failure(database):
    """ Test Object Class Program Activity gross_outlays_undelivered_fyb doesnt' equals
    ussgl480200_undelivered_or_fyb """

    op = ObjectClassProgramActivityFactory(gross_outlays_undelivered_fyb=1, ussgl480200_undelivered_or_fyb=0)

    assert number_of_errors(_FILE, database, models=[op]) == 1

