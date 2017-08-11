from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs22_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "correction_late_delete_ind"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ When provided, CorrectionLateDeleteIndicator must contain one of the following values: C, D, or L. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(correction_late_delete_ind="")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(correction_late_delete_ind=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(correction_late_delete_ind="c")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(correction_late_delete_ind="D")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(correction_late_delete_ind="L")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Test failure for when provided, CorrectionLateDeleteIndicator must contain one of the following values:
        C, D, or L. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(correction_late_delete_ind="A")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(correction_late_delete_ind="Z")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(correction_late_delete_ind="cd")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 3
