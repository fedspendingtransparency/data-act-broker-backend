from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import DUNS
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs31_detached_award_financial_assistance_6'


def test_column_headers(database):
    expected_subset = {"row_number", "assistance_type", "action_date", "action_type", "awardee_or_recipient_uniqu",
                       "business_types", "record_type", "correction_late_delete_ind"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """ For AssistanceType of 02, 03, 04, or 05 whose ActionDate is after October 1, 2010 and ActionType = A,
        AwardeeOrRecipientUniqueIdentifier must be active as of the ActionDate,
        unless the record is an aggregate record (RecordType=1) or individual recipient
        (BusinessTypes includes 'P'). This is a warning because CorrectionLateDeleteIndicator is C and the action
        date is before January 1, 2017."""
    duns_1 = DUNS(awardee_or_recipient_uniqu="111111111", registration_date="06/21/2015",
                  expiration_date="06/21/2016")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          assistance_type="02", action_date="06/22/2015",
                                                          record_type=2, business_types="A",
                                                          correction_late_delete_ind="C")
    # Different assistant type
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          assistance_type="01", action_date="06/20/2015",
                                                          record_type=2, business_types="A",
                                                          correction_late_delete_ind="C")
    # Before October 1, 2010
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          assistance_type="02", action_date="09/30/2010",
                                                          record_type=2, business_types="A",
                                                          correction_late_delete_ind="C")
    # Handled by d31_1
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          assistance_type="03", action_date="06/20/2015",
                                                          record_type=1, business_types="A",
                                                          correction_late_delete_ind="C")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          assistance_type="04", action_date="06/20/2015",
                                                          record_type=2, business_types="P",
                                                          correction_late_delete_ind="C")
    # Handled by d31_2
    det_award_6 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="", action_type="A",
                                                          assistance_type="05", action_date="06/20/2015",
                                                          record_type=2, business_types="A",
                                                          correction_late_delete_ind="C")
    det_award_7 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu=None, action_type="A",
                                                          assistance_type="02", action_date="06/20/2015",
                                                          record_type=2, business_types="A",
                                                          correction_late_delete_ind="C")
    # Handled by d31_3
    det_award_8 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="ABCDEFGHI", action_type="A",
                                                          assistance_type="03", action_date="06/20/2015",
                                                          record_type=2, business_types="A",
                                                          correction_late_delete_ind="C")
    # Handled by d31_4
    det_award_9 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111112", action_type="A",
                                                          assistance_type="04", action_date="06/20/2015",
                                                          record_type=2, business_types="A",
                                                          correction_late_delete_ind="C")
    # Handled by d31_5
    det_award_10 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                           assistance_type="05", action_date="06/20/2015",
                                                           record_type=2, business_types="A",
                                                           correction_late_delete_ind="D")
    det_award_11 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                           assistance_type="02", action_date="06/20/2017",
                                                           record_type=2, business_types="A",
                                                           correction_late_delete_ind="C")
    # handled in d31_7
    det_award_12 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="B",
                                                           assistance_type="03", action_date="06/20/2015",
                                                           record_type=2, business_types="A",
                                                           correction_late_delete_ind="C")
    # handled in d4
    det_award_13 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                           assistance_type="04", action_date="YYYYMMDD",
                                                           record_type=2, business_types="A",
                                                           correction_late_delete_ind="C")
    det_award_14 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="111111111", action_type="A",
                                                           assistance_type="05", action_date="AAAAAAAAAA",
                                                           record_type=2, business_types="A",
                                                           correction_late_delete_ind="C")

    errors = number_of_errors(_FILE, database, models=[duns_1, det_award_1, det_award_2, det_award_3,
                                                       det_award_4, det_award_5, det_award_6, det_award_7,
                                                       det_award_8, det_award_9, det_award_10, det_award_11,
                                                       det_award_12, det_award_13, det_award_14])
    assert errors == 0


def test_pubished_date_failure(database):
    """ Test failure for For AssistanceType of 02, 03, 04, or 05 whose ActionDate is after October 1, 2010
        and ActionType = A, AwardeeOrRecipientUniqueIdentifier must be active as of the ActionDate,
        unless the record is an aggregate record (RecordType=1) or individual recipient
        (BusinessTypes includes 'P'). This is a warning because CorrectionLateDeleteIndicator is C and the
        action date is before January 1, 2017."""

    duns_1 = DUNS(awardee_or_recipient_uniqu="111111111", registration_date="06/21/2015",
                  expiration_date="06/21/2016")
    det_award_1 = DetachedAwardFinancialAssistanceFactory(assistance_type="02", action_date="06/20/2015",
                                                          awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          record_type=2, business_types="A",
                                                          correction_late_delete_ind="C")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(assistance_type="03", action_date="06/22/2016",
                                                          awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          record_type=2, business_types="A",
                                                          correction_late_delete_ind="C")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(assistance_type="04", action_date="06/22/2016",
                                                          awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          record_type=2, business_types="A",
                                                          correction_late_delete_ind="C")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(assistance_type="05", action_date="06/22/2016",
                                                          awardee_or_recipient_uniqu="111111111", action_type="A",
                                                          record_type=2, business_types="A",
                                                          correction_late_delete_ind="C")

    errors = number_of_errors(_FILE, database, models=[duns_1, det_award_1, det_award_2, det_award_3,
                                                       det_award_4])
    assert errors == 4
