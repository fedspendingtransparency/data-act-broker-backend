from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import ExecutiveCompensation
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd44_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "awardee_or_recipient_uniqu", "action_type", "action_date"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """ Test valid. When ActionType = A, must be active in the System for Award Management (SAM) on the
        ActionDate of the award.
        When ActionType = B, C, or D, DUNS should exist in SAM but need not be active on the ActionDate
        (i.e., it may be expired).
    """

    exec_comp_1 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111111", activation_date="20170622")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          action_date="20170623")
    # inactive at action date but not A files
    exec_comp_2 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111112", activation_date="20170623")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111112", action_type="B",
                                                          action_date="20170622")
    exec_comp_3 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111113", activation_date="20170623")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111113", action_type="C",
                                                          action_date="20170622")
    exec_comp_4 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111114", activation_date="20170623")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111114", action_type="D",
                                                          action_date="20170622")

    errors = number_of_errors(_FILE, database, models=[det_award_1, exec_comp_1, det_award_2, exec_comp_2, det_award_3,
                                                       exec_comp_3, det_award_4, exec_comp_4])
    assert errors == 0


def test_pubished_date_failure(database):
    """ Test invalid. When ActionType = A, must be active in the System for Award Management (SAM) on the
        ActionDate of the award.
        When ActionType = B, C, or D, DUNS should exist in SAM but need not be active on the ActionDate
        (i.e., it may be expired).
    """

    exec_comp_1 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111111", activation_date="20170623")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          action_date="20170622")

    errors = number_of_errors(_FILE, database, models=[det_award_1, exec_comp_1])
    assert errors == 1
