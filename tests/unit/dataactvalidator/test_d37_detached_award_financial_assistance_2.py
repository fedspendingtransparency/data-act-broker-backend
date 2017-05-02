from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import CFDAProgram
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd37_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "cfda_number", "action_date", "action_type", "correction_late_delete_ind"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """ Test valid. For (ActionType = B, C, or D), the CFDA_Number need not be active as of the ActionDate.
        Not apply to those with CorrectionLateDeleteIndicator = C.
        If Archived Date <= Action Date <= Published Date, it passes validation (active).
    """

    cfda = CFDAProgram(program_number="12.345", published_date="20130427")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number="12.345", action_date='20110111',
                                                          action_type='B', correction_late_delete_ind="B")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number="12.345", action_date='20110111',
                                                          action_type='C', correction_late_delete_ind="D")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number="12.345", action_date='20110111',
                                                          action_type='D', correction_late_delete_ind=None)
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, cfda])
    assert errors == 0


def test_achived_date_success(database):
    """ Test invalid. For (ActionType = B, C, or D), the CFDA_Number need not be active as of the ActionDate.
        Not apply to those with CorrectionLateDeleteIndicator = C.
        If Archived Date < Action Date < Published Date, it passes validation (not active).
    """

    cfda = CFDAProgram(program_number="12.345", archived_date="20130427")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number="12.345", action_date='20150111',
                                                          action_type='B', correction_late_delete_ind="B")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number="12.345", action_date='20150111',
                                                          action_type='C', correction_late_delete_ind="D")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number="12.345", action_date='20150111',
                                                          action_type='D', correction_late_delete_ind=None)
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, cfda])
    assert errors == 0


def test_pubished_date_failure(database):
    """ Test invalid. For ActionType = A, the CFDA_Number must be active as of the ActionDate.
        Not apply to those with CorrectionLateDeleteIndicator = C.
        If Archived Date >= Action Date >= Published Date, it fails validation (active).
    """

    cfda = CFDAProgram(program_number="12.345", published_date="20130427")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number="12.345", action_date='20130428',
                                                          action_type='B', correction_late_delete_ind="B")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number="12.345", action_date='20130427',
                                                          action_type='C', correction_late_delete_ind="D")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number="12.345", action_date='20130428',
                                                          action_type='D', correction_late_delete_ind=None)
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, cfda])
    assert errors == 3


def test_achived_date_failure(database):
    """ Test valid. For (ActionType = B, C, or D), the CFDA_Number need not be active as of the ActionDate.
        Not apply to those with CorrectionLateDeleteIndicator = C.
        If Archived Date >= Action Date >= Published Date, it fails validation (active).
    """

    cfda = CFDAProgram(program_number="12.345", archived_date="20130427")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number="12.345", action_date='19990313',
                                                          action_type='C', correction_late_delete_ind="")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number="12.345", action_date='20130427',
                                                          action_type='C', correction_late_delete_ind="D")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number="12.345", action_date='20130426',
                                                          action_type='D', correction_late_delete_ind=None)
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, cfda])
    assert errors == 3
