from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b7_object_class_program_activity_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'gross_outlays_delivered_or_cpe', 'ussgl490200_delivered_orde_cpe',
                       'ussgl490800_authority_outl_cpe', 'ussgl497200_downward_adjus_cpe',
                       'ussgl498200_upward_adjustm_cpe'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test Object Class Program Activity gross_outlays_delivered_or_cpe equals ussgl490200_delivered_orde_cpe +
    ussgl490800_authority_outl_cpe + ussgl497200_downward_adjus_cpe + ussgl498200_upward_adjustm_cpe """

    op = ObjectClassProgramActivityFactory(gross_outlays_delivered_or_cpe=4, ussgl490200_delivered_orde_cpe=1,
                                           ussgl490800_authority_outl_cpe=1, ussgl497200_downward_adjus_cpe=1,
                                           ussgl498200_upward_adjustm_cpe=1)

    assert number_of_errors(_FILE, database, models=[op]) == 0


def test_failure(database):
    """ Test Object Class Program Activity gross_outlays_delivered_or_cpe doesn't equals ussgl490200_delivered_orde_cpe +
    ussgl490800_authority_outl_cpe + ussgl497200_downward_adjus_cpe + ussgl498200_upward_adjustm_cpe """

    op = ObjectClassProgramActivityFactory(gross_outlays_delivered_or_cpe=1, ussgl490200_delivered_orde_cpe=1,
                                           ussgl490800_authority_outl_cpe=1, ussgl497200_downward_adjus_cpe=1,
                                           ussgl498200_upward_adjustm_cpe=1)

    assert number_of_errors(_FILE, database, models=[op]) == 1

