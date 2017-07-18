from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd45_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "awardee_or_recipient_uniqu"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ When provided, all characters in AwardeeOrRecipientUniqueIdentifier must be numeric.
        If, for example, "ABCDEFGHI" is provided, it should trigger a format error. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="000000001")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="103493922")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="100000000")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Test failure for when provided, all characters in AwardeeOrRecipientUniqueIdentifier must be numeric.
        If, for example, "ABCDEFGHI" is provided, it should trigger a format error."""
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="00000000A")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="ABCDEFGHI")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="AAA")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="AAAAAAAAAAA")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 4
