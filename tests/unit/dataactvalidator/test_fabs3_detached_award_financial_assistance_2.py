from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs3_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'action_type', 'record_type', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests if ActionType is one of the following values: “A”, “B”, “C”, “D”, or blank. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(action_type='a', correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(action_type='B', correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(action_type='c', correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(action_type='D', correction_delete_indicatr='C')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(action_type='', correction_delete_indicatr='')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(action_type=None, correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    det_award_7 = DetachedAwardFinancialAssistanceFactory(action_type='Thing', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, det_award_7])
    assert errors == 0


def test_failure(database):
    """ Tests if ActionType is not one of the following values: “A”, “B”, “C”, “D”, or blank. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(action_type='random', correction_delete_indicatr='c')

    errors = number_of_errors(_FILE, database, models=[det_award_1])
    assert errors == 1
