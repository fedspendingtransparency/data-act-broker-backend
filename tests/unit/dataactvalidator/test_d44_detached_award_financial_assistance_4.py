from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import ExecutiveCompensation
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd44_detached_award_financial_assistance_4'


def test_column_headers(database):
    expected_subset = {"row_number", "assistance_type", "action_date", "action_type", "awardee_or_recipient_uniqu"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """ For AssistanceType of 02, 03, 04, or 05 whose ActionDate is after October 1, 2010 and ActionType = A,
        the DUNS must be active as of the ActionDate."""
    exec_comp_1 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111111", activation_date="06/21/2017",
                                        expiration_date="06/21/2018")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          assistance_type="02", action_date="06/22/2017")
    # Different assistant type
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          assistance_type="01", action_date="06/20/2017")
    # Before October 1, 2010
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          assistance_type="02", action_date="09/30/2010")
    # Handled by d44_1
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="", action_type="A",
                                                          assistance_type="02", action_date="06/20/2010")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu=None, action_type="A",
                                                          assistance_type="02", action_date="06/20/2010")
    # Handled by d44_2
    det_award_6 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="ABCDEFGHI", action_type="A",
                                                          assistance_type="02", action_date="06/20/2010")
    # Handled by d44_3
    det_award_7 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111112", action_type="A",
                                                          assistance_type="02", action_date="06/20/2010")
    # handled in d44_5
    det_award_8 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="B",
                                                          assistance_type="03", action_date="06/20/2017")
    # handled in d4
    det_award_9 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          assistance_type="03", action_date="YYYYMMDD")
    det_award_10 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                           assistance_type="03", action_date="AAAAAAAAAA")

    errors = number_of_errors(_FILE, database, models=[exec_comp_1, det_award_1, det_award_2, det_award_3,
                                                       det_award_4, det_award_5, det_award_6, det_award_7,
                                                       det_award_8, det_award_9, det_award_10])
    assert errors == 0


def test_pubished_date_failure(database):
    """ Test failure for AssistanceType of 02, 03, 04, or 05 whose ActionDate is after October 1, 2010
        and ActionType = A, the DUNS must be active as of the ActionDate."""

    exec_comp_1 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111111", activation_date="06/21/2017",
                                        expiration_date="06/21/2018")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(assistance_type="02", action_date="06/20/2017",
                                                          awardee_or_recipient_uniqu="111111111", action_type="A")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(assistance_type="03", action_date="06/22/2018",
                                                          awardee_or_recipient_uniqu="111111111", action_type="A")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(assistance_type="04", action_date="06/22/2018",
                                                          awardee_or_recipient_uniqu="111111111", action_type="A")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(assistance_type="05", action_date="06/22/2018",
                                                          awardee_or_recipient_uniqu="111111111", action_type="A")

    errors = number_of_errors(_FILE, database, models=[exec_comp_1, det_award_1, det_award_2, det_award_3,
                                                       det_award_4])
    assert errors == 4
