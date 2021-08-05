from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs46_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'indirect_federal_sharing', 'assistance_type', 'action_date',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test IndirectCostFederalShareAmount is required for grants and cooperative agreements
        (AssistanceType = 02, 03, 04, or 05). This only applies to award actions with ActionDate on or after April 4,
        2022.
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(indirect_federal_sharing=123, assistance_type='02',
                                                          action_date='05/05/2022')

    # Doesn't care about other assistance types
    det_award_2 = DetachedAwardFinancialAssistanceFactory(indirect_federal_sharing=None, assistance_type='09',
                                                          action_date='05/05/2022')

    # Doesn't care about earlier dates
    det_award_3 = DetachedAwardFinancialAssistanceFactory(indirect_federal_sharing=None, assistance_type='03',
                                                          action_date='05/05/2021')

    # Still doesn't trigger when not blank for other assistance types
    det_award_4 = DetachedAwardFinancialAssistanceFactory(indirect_federal_sharing=123, assistance_type='09')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Test failure IndirectCostFederalShareAmount is required for grants and cooperative agreements
        (AssistanceType = 02, 03, 04, or 05). This only applies to award actions with ActionDate on or after April 4,
        2022.
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(indirect_federal_sharing=None, assistance_type='02',
                                                          action_date='05/05/2022')
    errors = number_of_errors(_FILE, database, models=[det_award_1])
    assert errors == 1
