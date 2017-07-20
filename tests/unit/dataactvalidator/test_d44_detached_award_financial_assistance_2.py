from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import ExecutiveCompensation
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd44_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "assistance_type", "action_date", "action_type", "awardee_or_recipient_uniqu"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """ Test valid. When ActionType = A, must be active in the System for Award Management (SAM) on the
        ActionDate of the award.
        AwardeeOrRecipientUniqueIdentifier is required for AssistanceType of 02, 03, 04, or 05 whose
        ActionDate after October 1, 2010.
        When ActionType = B, C, or D, DUNS should exist in SAM but need not be active on the ActionDate
        (i.e., it may be expired).
    """
    exec_comp_1 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111111", activation_date="06/21/2017")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          assistance_type="02", action_date="06/23/2017")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          assistance_type="03", action_date="YYYYMMDD")
    # inactive at action date but not A files
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="B",
                                                          assistance_type="03", action_date="06/21/2017")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="C",
                                                          assistance_type="04", action_date="06/21/2017")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="D",
                                                          assistance_type="05", action_date="06/21/2017")
    det_award_6 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="11111111B", action_type="D",
                                                          assistance_type="05", action_date="09/21/1990")
    det_award_7 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          assistance_type="03", action_date="AAAAAAAAAA")

    errors = number_of_errors(_FILE, database, models=[exec_comp_1, det_award_1, det_award_2, det_award_3,
                                                       det_award_4, det_award_5, det_award_6, det_award_7])
    assert errors == 0


def test_pubished_date_failure(database):
    """ Test invalid. When ActionType = A, must be active in the System for Award Management (SAM) on the
        ActionDate of the award.
        AwardeeOrRecipientUniqueIdentifier is required for AssistanceType of 02, 03, 04, or 05 whose
        ActionDate after October 1, 2010.
        When ActionType = B, C, or D, DUNS should exist in SAM but need not be active on the ActionDate
        (i.e., it may be expired).
    """

    exec_comp_1 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111111", activation_date="06/23/2017")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(assistance_type="03", action_date="06/22/2017",
                                                          awardee_or_recipient_uniqu="111111111", action_type="A")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(assistance_type="03", action_date="06/23/2017",
                                                          awardee_or_recipient_uniqu="AAAAAAAAA", action_type="A")

    errors = number_of_errors(_FILE, database, models=[exec_comp_1, det_award_1, det_award_2])
    assert errors == 2
