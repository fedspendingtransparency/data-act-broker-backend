from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'b24_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'row_number', 'disaster_emergency_fund_code', 'uniqueid_TAS',
                       'uniqueid_DisasterEmergencyFundCode'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test DEFC values must be A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T or 9 (plus future codes as
        determined by OMB). DEFC cannot be blank.
    """
    op1 = ObjectClassProgramActivityFactory(disaster_emergency_fund_code='a')
    op2 = ObjectClassProgramActivityFactory(disaster_emergency_fund_code='Q')
    op3 = ObjectClassProgramActivityFactory(disaster_emergency_fund_code='9')

    errors = number_of_errors(_FILE, database, models=[op1, op2, op3])
    assert errors == 0


def test_failure(database):
    """ Test fail DEFC values must be A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T or 9 (plus future
        codes as determined by OMB). DEFC cannot be blank.
    """
    op1 = ObjectClassProgramActivityFactory(disaster_emergency_fund_code='z')
    op2 = ObjectClassProgramActivityFactory(disaster_emergency_fund_code='3')
    op3 = ObjectClassProgramActivityFactory(disaster_emergency_fund_code='AA')
    op4 = ObjectClassProgramActivityFactory(disaster_emergency_fund_code='')

    errors = number_of_errors(_FILE, database, models=[op1, op2, op3, op4])
    assert errors == 4
