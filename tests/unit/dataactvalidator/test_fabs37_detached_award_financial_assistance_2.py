from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import CFDAProgram
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs37_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'cfda_number', 'action_date', 'action_type', 'correction_delete_indicatr'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """ Test valid. For (ActionType = B, C, or D), the CFDA_Number need NOT be active as of the ActionDate.
        Not apply to those with CorrectionDeleteIndicator = C.
        Active date: publish_date <= action_date <= archive_date (Fails validation if active).
    """

    cfda = CFDAProgram(program_number=12.340, published_date='20130427', archived_date='')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20140528',
                                                          action_type='b', correction_delete_indicatr='B')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20140428',
                                                          action_type='c', correction_delete_indicatr='')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20140428',
                                                          action_type='D', correction_delete_indicatr=None)
    # Ignore correction delete indicator of D
    det_award_4 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20120528',
                                                          action_type='B', correction_delete_indicatr='d')
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, cfda])
    assert errors == 0

    cfda = CFDAProgram(program_number=12.350, published_date='20130427', archived_date='20140427')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.350', action_date='20130528',
                                                          action_type='b', correction_delete_indicatr='B')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.350', action_date='20130428',
                                                          action_type='C', correction_delete_indicatr='')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.350', action_date='20130428',
                                                          action_type='d', correction_delete_indicatr=None)
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, cfda])
    assert errors == 0


def test_pubished_date_failure(database):
    """ Test invalid. For (ActionType = B, C, or D), the CFDA_Number need NOT be active as of the ActionDate.
        Not apply to those with CorrectionDeleteIndicator = C.
        Active date: publish_date <= action_date <= archive_date (Fails validation if active).
        If action date is < published_date, should trigger a warning.
    """

    cfda = CFDAProgram(program_number=12.340, published_date='20130427', archived_date='')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20120528',
                                                          action_type='b', correction_delete_indicatr='B')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20120427',
                                                          action_type='C', correction_delete_indicatr='')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.340', action_date='20120428',
                                                          action_type='d', correction_delete_indicatr=None)
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, cfda])
    assert errors == 3

    cfda = CFDAProgram(program_number=12.350, published_date='20130427', archived_date='20140528')
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.350', action_date='20120528',
                                                          action_type='B', correction_delete_indicatr='B')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.350', action_date='20150427',
                                                          action_type='c', correction_delete_indicatr='')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number='12.350', action_date='20150428',
                                                          action_type='D', correction_delete_indicatr=None)
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, cfda])
    assert errors == 3
