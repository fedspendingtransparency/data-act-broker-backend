from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import ExecutiveCompensation
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd44_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "awardee_or_recipient_uniqu"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """ Test valid. For (ActionType = B, C, or D), the CFDA_Number need NOT be active as of the
        ActionDate.
        Not apply to those with CorrectionLateDeleteIndicator = C.
        Active date: publish_date <= action_date <= archive_date (Fails validation if active).
    """

    exec_comp_1 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111111")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111")

    errors = number_of_errors(_FILE, database, models=[det_award_1, exec_comp_1])
    assert errors == 0


def test_pubished_date_failure(database):
    """ Test invalid. For (ActionType = B, C, or D), the CFDA_Number need NOT be active as of the
        ActionDate.
        Not apply to those with CorrectionLateDeleteIndicator = C.
        Active date: publish_date <= action_date <= archive_date (Fails validation if active).
        If action date is < published_date, should trigger a warning.
    """

    exec_comp_1 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111111")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111112")

    errors = number_of_errors(_FILE, database, models=[det_award_1, exec_comp_1])
    assert errors == 1
