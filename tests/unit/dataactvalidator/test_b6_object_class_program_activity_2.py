from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b6_object_class_program_activity_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'gross_outlays_undelivered_cpe', 'ussgl480200_undelivered_or_cpe',
                       'ussgl483200_undelivered_or_cpe', 'ussgl487200_downward_adjus_cpe',
                       'ussgl488200_upward_adjustm_cpe'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test Object Class Program Activity gross_outlays_undelivered_cpe equals ussgl480200_undelivered_or_cpe +
    ussgl483200_undelivered_or_cpe + ussgl487200_downward_adjus_cpe + ussgl488200_upward_adjustm_cpe """

    op = ObjectClassProgramActivityFactory(gross_outlays_undelivered_cpe=4, ussgl480200_undelivered_or_cpe=1,
                                           ussgl483200_undelivered_or_cpe=1, ussgl487200_downward_adjus_cpe=1,
                                           ussgl488200_upward_adjustm_cpe=1)

    assert number_of_errors(_FILE, database, models=[op]) == 0


def test_failure(database):
    """ Test Object Class Program Activity gross_outlays_undelivered_cpe doesn't equals ussgl480200_undelivered_or_cpe +
    ussgl483200_undelivered_or_cpe + ussgl487200_downward_adjus_cpe + ussgl488200_upward_adjustm_cpe """

    op = ObjectClassProgramActivityFactory(gross_outlays_undelivered_cpe=1, ussgl480200_undelivered_or_cpe=1,
                                           ussgl483200_undelivered_or_cpe=1, ussgl487200_downward_adjus_cpe=1,
                                           ussgl488200_upward_adjustm_cpe=1)

    assert number_of_errors(_FILE, database, models=[op]) == 1

