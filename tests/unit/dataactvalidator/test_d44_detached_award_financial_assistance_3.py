from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import ExecutiveCompensation
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd44_detached_award_financial_assistance_3'


def test_column_headers(database):
    expected_subset = {"row_number", "assistance_type", "action_date", "awardee_or_recipient_uniqu"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """ For AssistanceType of 02, 03, 04, or 05 whose ActionDate is after October 1, 2010,
        the DUNS must be found in our records."""

    exec_comp_1 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111111")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111",
                                                          assistance_type="02", action_date="10/02/2010")
    # Different assistance type
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111112",
                                                          assistance_type="01", action_date="10/02/2010")
    # Before October 1, 2010
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111112",
                                                          assistance_type="02", action_date="09/30/2010")
    # Handled by d44_1
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="",
                                                          assistance_type="02", action_date="10/02/2010")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu=None,
                                                          assistance_type="02", action_date="10/02/2010")
    # Handled by d44_2
    det_award_6 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="ABCDEFGHI",
                                                          assistance_type="02", action_date="10/02/2010")

    errors = number_of_errors(_FILE, database, models=[exec_comp_1, det_award_1, det_award_2, det_award_3,
                                                       det_award_4, det_award_5, det_award_6])
    assert errors == 0


def test_pubished_date_failure(database):
    """ Test invalid for AssistanceType of 02, 03, 04, or 05 whose ActionDate is after October 1, 2010,
        the DUNS must be found in our records."""

    exec_comp_1 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111111")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111112",
                                                          assistance_type="02", action_date="10/02/2010")

    errors = number_of_errors(_FILE, database, models=[exec_comp_1, det_award_1])
    assert errors == 1
