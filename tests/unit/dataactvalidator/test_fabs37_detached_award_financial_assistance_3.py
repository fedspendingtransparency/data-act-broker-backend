from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import CFDAProgram
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs37_detached_award_financial_assistance_3'


def test_column_headers(database):
    expected_subset = {"row_number", "cfda_number"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test valid. For ActionType = A, the CFDA_Number must be active as of the ActionDate.
        Not apply to those with CorrectionLateDeleteIndicator = C.
        If publish_date <= action_date <= archived_date, it passes validation (active).
    """

    cfda = CFDAProgram(program_number=12.340)
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number="12.340")

    errors = number_of_errors(_FILE, database, models=[det_award_1, cfda])
    assert errors == 0


def test_failure(database):
    """ Test that the cfda_number exists 
    """

    # test for cfda_number that doesn't exist in the table
    cfda = CFDAProgram(program_number=12.340, published_date="20130427", archived_date="")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(cfda_number="54.321", action_date='20140111',
                                                          action_type='A', correction_late_delete_ind="B")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(cfda_number="AB.CDE", action_date='20140111',
                                                          action_type='A', correction_late_delete_ind=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(cfda_number="11.111", action_date='20130528',
                                                          action_type='B', correction_late_delete_ind="B")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, cfda])
    assert errors == 3
