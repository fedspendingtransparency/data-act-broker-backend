from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs9_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "record_type", "awardee_or_recipient_legal"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test that awardee_or_recipient_legal "MULTIPLE RECIPIENTS" contains for aggregate records and record type
        2 doesn't affect success"""
    det_award = DetachedAwardFinancialAssistanceFactory(record_type=1, awardee_or_recipient_legal="MULTIPLE RECIPIENTS")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=2, awardee_or_recipient_legal="TEST AGENCY")

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 0


def test_failure(database):
    """ Test that awardee_or_recipient_legal without "MULTIPLE RECIPIENTS" for record type 1 fails"""

    det_award = DetachedAwardFinancialAssistanceFactory(record_type=1,
                                                        awardee_or_recipient_legal="MULTIPLE RECIPIENTS2")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=1, awardee_or_recipient_legal="other")

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
