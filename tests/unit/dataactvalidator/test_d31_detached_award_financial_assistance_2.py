from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd31_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "assistance_type", "action_date", "awardee_or_recipient_uniqu", "business_types",
                       "record_type"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test success for AwardeeOrRecipientUniqueIdentifier is required for AssistanceType of 02, 03, 04, or 05 whose
    ActionDate after October 1, 2010, unless the record is an aggregate record (RecordType=1) or individual recipient
    (BusinessTypes includes 'P') """
    det_award_01 = DetachedAwardFinancialAssistanceFactory(assistance_type="02", record_type=2, business_types="AbC",
                                                           awardee_or_recipient_uniqu='test', action_date="10/02/2010")
    det_award_02 = DetachedAwardFinancialAssistanceFactory(assistance_type="03", record_type=3, business_types="aBc",
                                                           awardee_or_recipient_uniqu='test', action_date="10/02/2010")
    det_award_03 = DetachedAwardFinancialAssistanceFactory(assistance_type="04", record_type=4, business_types="AbC",
                                                           awardee_or_recipient_uniqu='test', action_date="10/02/2010")
    det_award_04 = DetachedAwardFinancialAssistanceFactory(assistance_type="05", record_type=3, business_types="aBc",
                                                           awardee_or_recipient_uniqu='test', action_date="10/02/2010")
    det_award_05 = DetachedAwardFinancialAssistanceFactory(assistance_type="02", record_type=2, business_types="AbC",
                                                           awardee_or_recipient_uniqu=None, action_date="09/01/2010")
    det_award_06 = DetachedAwardFinancialAssistanceFactory(assistance_type="03", record_type=3, business_types="aBc",
                                                           awardee_or_recipient_uniqu='', action_date="09/01/2010")
    det_award_07 = DetachedAwardFinancialAssistanceFactory(assistance_type="04", record_type=4, business_types="AbC",
                                                           awardee_or_recipient_uniqu=None, action_date="09/01/2010")
    det_award_08 = DetachedAwardFinancialAssistanceFactory(assistance_type="05", record_type=3, business_types="AbC",
                                                           awardee_or_recipient_uniqu=None, action_date="09/01/2010")
    det_award_09 = DetachedAwardFinancialAssistanceFactory(assistance_type="04", record_type=2, business_types="AbC",
                                                           awardee_or_recipient_uniqu='test', action_date="09/01/2010")
    det_award_10 = DetachedAwardFinancialAssistanceFactory(assistance_type="02", record_type=3, business_types="aBp",
                                                           awardee_or_recipient_uniqu=None, action_date="10/02/2010")
    det_award_11 = DetachedAwardFinancialAssistanceFactory(assistance_type="04", record_type=1, business_types="AbC",
                                                           awardee_or_recipient_uniqu='', action_date="10/02/2010")
    det_award_12 = DetachedAwardFinancialAssistanceFactory(assistance_type="06", record_type=4, business_types="aBc",
                                                           awardee_or_recipient_uniqu='', action_date="10/02/2010")

    errors = number_of_errors(_FILE, database, models=[det_award_01, det_award_02, det_award_03, det_award_04,
                                                       det_award_05, det_award_06, det_award_07, det_award_08,
                                                       det_award_09, det_award_10, det_award_11, det_award_12])
    assert errors == 0


def test_failure(database):
    """ Test failure for AwardeeOrRecipientUniqueIdentifier is required for AssistanceType of 02, 03, 04, or 05 whose
    ActionDate after October 1, 2010, , unless the record is an aggregate record (RecordType=1) or individual recipient
    (BusinessTypes includes 'P') """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(assistance_type="02", record_type=2, business_types="AbC",
                                                          awardee_or_recipient_uniqu=None, action_date="10/02/2010")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(assistance_type="03", record_type=3, business_types="aBc",
                                                          awardee_or_recipient_uniqu='', action_date="10/02/2010")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(assistance_type="04", record_type=4, business_types="AbC",
                                                          awardee_or_recipient_uniqu=None, action_date="10/02/2010")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(assistance_type="05", record_type=3, business_types="aBc",
                                                          awardee_or_recipient_uniqu='', action_date="10/02/2010")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 4
