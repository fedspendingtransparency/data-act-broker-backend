from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'b5_object_class_program_activity_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'prior_year_adjustment', 'gross_outlay_amount_by_pro_fyb',
                       'gross_outlays_undelivered_fyb', 'gross_outlays_delivered_or_fyb', 'difference', 'uniqueid_TAS',
                       'uniqueid_DisasterEmergencyFundCode', 'uniqueid_ProgramActivityCode', 'uniqueid_ObjectClass'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that Object Class Program activity gross_outlays_delivered_or_fyb + gross_outlays_undelivered_fyb
        equals gross_outlay_amount_by_pro_fyb for the same TAS/DEFC combination where PYA = "X".
    """

    op = ObjectClassProgramActivityFactory(gross_outlay_amount_by_pro_fyb=2, gross_outlays_undelivered_fyb=1,
                                           gross_outlays_delivered_or_fyb=1, prior_year_adjustment='X')
    # Different values, Different PYA
    op2 = ObjectClassProgramActivityFactory(gross_outlay_amount_by_pro_fyb=0, gross_outlays_undelivered_fyb=1,
                                            gross_outlays_delivered_or_fyb=1)

    assert number_of_errors(_FILE, database, models=[op, op2]) == 0


def test_failure(database):
    """ Tests that Object Class Program activity gross_outlays_delivered_or_fyb + gross_outlays_undelivered_fyb
        doesn't equal gross_outlay_amount_by_pro_fyb for the same TAS/DEFC combination where PYA = "X".
    """

    op = ObjectClassProgramActivityFactory(gross_outlay_amount_by_pro_fyb=1, gross_outlays_undelivered_fyb=1,
                                           gross_outlays_delivered_or_fyb=1, prior_year_adjustment='x')

    assert number_of_errors(_FILE, database, models=[op]) == 1
