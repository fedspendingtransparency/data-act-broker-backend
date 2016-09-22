from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors,query_columns


_FILE = 'a31_object_class_program_activity'
_TAS = 'a31_object_class_program_activity_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'availability_type_code',
                       'beginning_period_of_availa', 'ending_period_of_availabil'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that Beginning Period of Availability and Ending Period of Availability are blank
    if Availability Type Code = X """
    tas = "".join([_TAS, "_success"])

    op1 = ObjectClassProgramActivityFactory(availability_type_code='x', beginning_period_of_availa=None,
                                ending_period_of_availabil=None)
    op2 = ObjectClassProgramActivityFactory(availability_type_code='X', beginning_period_of_availa=None,
                                ending_period_of_availabil=None)

    assert number_of_errors(_FILE, database, models=[op1, op2]) == 0


def test_failure(database):
    """ Tests that Beginning Period of Availability and Ending Period of Availability are not blank
    if Availability Type Code = X """
    tas = "".join([_TAS, "_failure"])

    op1 = ObjectClassProgramActivityFactory(availability_type_code='x', beginning_period_of_availa='Today',
                                ending_period_of_availabil='Today')
    op2 = ObjectClassProgramActivityFactory(availability_type_code='x', beginning_period_of_availa='Today',
                                ending_period_of_availabil=None)
    op3 = ObjectClassProgramActivityFactory(availability_type_code='x', beginning_period_of_availa=None,
                                ending_period_of_availabil='Today')
    op4 = ObjectClassProgramActivityFactory(availability_type_code='X', beginning_period_of_availa='Today',
                                ending_period_of_availabil='Today')
    op5 = ObjectClassProgramActivityFactory(availability_type_code='X', beginning_period_of_availa='Today',
                                ending_period_of_availabil=None)
    op6 = ObjectClassProgramActivityFactory(availability_type_code='X', beginning_period_of_availa=None,
                                ending_period_of_availabil='Today')

    assert number_of_errors(_FILE, database, models=[op1, op2, op3, op4, op5, op6]) == 6
