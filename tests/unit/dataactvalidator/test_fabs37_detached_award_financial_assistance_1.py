from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import CFDAProgram
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs37_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'cfda_number', 'action_date', 'action_type', 'correction_delete_indicatr',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual



def test_success(database):
    """ Test valid. For new (ActionType = A) or mixed aggregate (ActionType = E) assistance awards specifically, the
        CFDA_Number must be active as of the ActionDate. This does not apply to correction records
        (those with CorrectionDeleteIndicator = C and delete records).
    """

    cfda = CFDAProgram(program_number=12.340, published_date='20130427', archived_date='')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20140111',
                                                          action_type='a', correction_delete_indicatr='B')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20140111',
                                                          action_type='E', correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20130427',
                                                          action_type='a', correction_delete_indicatr='B')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20110111',
                                                          action_type='B', correction_delete_indicatr='B')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20110111',
                                                          action_type='A', correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    det_award_6 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20110111',
                                                          action_type='e', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4,
                                                       det_award_5, det_award_6, cfda])
    assert errors == 0

    cfda = CFDAProgram(program_number=12.350, published_date='20130427', archived_date='20150427')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.350', action_date='20140111',
                                                          action_type='E', correction_delete_indicatr='B')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.350', action_date='20140111',
                                                          action_type='a', correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.350', action_date='20130427',
                                                          action_type='A', correction_delete_indicatr='B')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.350', action_date='20110111',
                                                          action_type='B', correction_delete_indicatr='B')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.350', action_date='20110111',
                                                          action_type='e', correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4,
                                                       det_award_5, cfda])
    assert errors == 0


def test_failure(database):
    """ Test invalid. For new (ActionType = A) or mixed aggregate (ActionType = E) assistance awards specifically, the
        CFDA_Number must be active as of the ActionDate. This does not apply to correction records
        (those with CorrectionDeleteIndicator = C and delete records).
    """

    cfda = CFDAProgram(program_number=12.340, published_date='20130427', archived_date='')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20120111',
                                                          action_type='e', correction_delete_indicatr='B')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20120111',
                                                          action_type='A', correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20120111',
                                                          action_type='a', correction_delete_indicatr='B')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20120111',
                                                          action_type='E', correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, cfda])
    assert errors == 4
