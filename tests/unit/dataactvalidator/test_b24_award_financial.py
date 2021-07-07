from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactcore.factories.domain import DEFCFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'b24_award_financial'


def test_column_headers(database):
    expected_subset = {'row_number', 'disaster_emergency_fund_code', 'uniqueid_TAS',
                       'uniqueid_DisasterEmergencyFundCode'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test DEFC values must be A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, or T (plus future codes as
        determined by OMB). DEFC cannot be blank.
    """
    op1 = AwardFinancialFactory(disaster_emergency_fund_code='a')
    op2 = AwardFinancialFactory(disaster_emergency_fund_code='Q')
    op3 = AwardFinancialFactory(disaster_emergency_fund_code='g')
    defc1 = DEFCFactory(code='A')
    defc2 = DEFCFactory(code='Q')
    defc3 = DEFCFactory(code='G')

    errors = number_of_errors(_FILE, database, models=[op1, op2, op3, defc1, defc2, defc3])
    assert errors == 0


def test_failure(database):
    """ Test fail DEFC values must be A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, or T (plus future
        codes as determined by OMB). DEFC cannot be blank.
    """
    op1 = AwardFinancialFactory(disaster_emergency_fund_code='z')
    op2 = AwardFinancialFactory(disaster_emergency_fund_code='3')
    op3 = AwardFinancialFactory(disaster_emergency_fund_code='AA')
    op4 = AwardFinancialFactory(disaster_emergency_fund_code='')

    errors = number_of_errors(_FILE, database, models=[op1, op2, op3, op4])
    assert errors == 4
