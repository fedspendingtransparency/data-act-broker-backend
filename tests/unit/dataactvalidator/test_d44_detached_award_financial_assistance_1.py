from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import ExecutiveCompensation
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd44_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "assistance_type", "action_date", "awardee_or_recipient_uniqu"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """ Test valid.
        Must be a valid 9-digit DUNS number
        AwardeeOrRecipientUniqueIdentifier is required for AssistanceType of 02, 03, 04, or 05 whose
        ActionDate after October 1, 2010.
    """

    exec_comp_1 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111111")
    det_award_01 = DetachedAwardFinancialAssistanceFactory(assistance_type="02", action_date="10/02/2010",
                                                           awardee_or_recipient_uniqu="111111111")
    det_award_02 = DetachedAwardFinancialAssistanceFactory(assistance_type="03", action_date="10/02/2010",
                                                           awardee_or_recipient_uniqu="111111111")
    det_award_03 = DetachedAwardFinancialAssistanceFactory(assistance_type="04", action_date="10/02/2010",
                                                           awardee_or_recipient_uniqu="111111111")
    det_award_04 = DetachedAwardFinancialAssistanceFactory(assistance_type="05", action_date="10/02/2010",
                                                           awardee_or_recipient_uniqu="111111111")
    det_award_05 = DetachedAwardFinancialAssistanceFactory(assistance_type="02", action_date="09/01/2010",
                                                           awardee_or_recipient_uniqu="111111111")
    det_award_06 = DetachedAwardFinancialAssistanceFactory(assistance_type="03", action_date="09/01/2010",
                                                           awardee_or_recipient_uniqu="111111111")
    det_award_07 = DetachedAwardFinancialAssistanceFactory(assistance_type="04", action_date="09/01/2010",
                                                           awardee_or_recipient_uniqu="111111111")
    det_award_08 = DetachedAwardFinancialAssistanceFactory(assistance_type="05", action_date="09/01/2010",
                                                           awardee_or_recipient_uniqu="111111111")
    det_award_09 = DetachedAwardFinancialAssistanceFactory(assistance_type="04", action_date="09/01/2010",
                                                           awardee_or_recipient_uniqu="111111111")
    det_award_10 = DetachedAwardFinancialAssistanceFactory(assistance_type="04", action_date="09/01/2010",
                                                           awardee_or_recipient_uniqu="AAAAAAAAA")
    det_award_11 = DetachedAwardFinancialAssistanceFactory(assistance_type="04", action_date="09/01/2010",
                                                           awardee_or_recipient_uniqu="")
    det_award_12 = DetachedAwardFinancialAssistanceFactory(assistance_type="06", action_date="10/02/2010",
                                                           awardee_or_recipient_uniqu="111111111")
    det_award_13 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111")
    det_award_14 = DetachedAwardFinancialAssistanceFactory(assistance_type="05", action_date="AAAAAAAAAA",
                                                           awardee_or_recipient_uniqu="111111112")

    errors = number_of_errors(_FILE, database, models=[exec_comp_1, det_award_01, det_award_02, det_award_03,
                                                       det_award_04, det_award_05, det_award_06, det_award_07,
                                                       det_award_08, det_award_09, det_award_10, det_award_11,
                                                       det_award_12, det_award_13, det_award_14])
    assert errors == 0


def test_pubished_date_failure(database):
    """ Test invalid.
        Must be a valid 9-digit DUNS number
        AwardeeOrRecipientUniqueIdentifier is required for AssistanceType of 02, 03, 04, or 05 whose
        ActionDate after October 1, 2010.
    """

    exec_comp_1 = ExecutiveCompensation(awardee_or_recipient_uniqu="111111111")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(assistance_type="02", action_date="10/02/2010",
                                                          awardee_or_recipient_uniqu=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(assistance_type="03", action_date="10/02/2010",
                                                          awardee_or_recipient_uniqu='')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(assistance_type="04", action_date="10/02/2010",
                                                          awardee_or_recipient_uniqu=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(assistance_type="05", action_date="10/02/2010",
                                                          awardee_or_recipient_uniqu='')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(assistance_type="05", action_date="10/02/2010",
                                                          awardee_or_recipient_uniqu="111111112")

    errors = number_of_errors(_FILE, database, models=[exec_comp_1, det_award_1, det_award_2, det_award_3,
                                                       det_award_4, det_award_5])
    assert errors == 5
