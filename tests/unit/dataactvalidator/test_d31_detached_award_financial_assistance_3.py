from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd31_detached_award_financial_assistance_3'


def test_column_headers(database):
    expected_subset = {"row_number", "assistance_type", "action_date", "awardee_or_recipient_uniqu",
                       "business_types", "record_type"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ If the record is not an aggregate record (RecordType=1) or individual recipient (BusinessTypes includes 'P')
        and AwardeeOrRecipientUniqueIdentifier is provided, it must be nine digits."""
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="000000001", record_type=2,
                                                          business_types="A")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="103493922", record_type=2,
                                                          business_types="A")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="100000000", record_type=2,
                                                          business_types="A")

    # Handled by d31_1
    det_award_5 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="ABCDEFGHI", record_type=1)
    det_award_6 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="0000", business_types="P")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4,
                                                       det_award_5, det_award_6])
    assert errors == 0


def test_failure(database):
    """ Test failure for if the record is not an aggregate record (RecordType=1) or individual recipient
        (BusinessTypes includes 'P') and AwardeeOrRecipientUniqueIdentifier is provided, it must be nine digits. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="00000000A", record_type=2,
                                                          business_types="A")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="ABCDEFGHI", record_type=2,
                                                          business_types="A")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="AAA", record_type=2,
                                                          business_types="A")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="AAAAAAAAAAA", record_type=2,
                                                          business_types="A")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 4
