from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabsreq9_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'awardee_or_recipient_legal', 'correction_delete_indicatr'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test AwardeeOrRecipientLegalEntityName is required for all submissions except delete records. """

    det_award = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='C',
                                                        awardee_or_recipient_legal='REDACTED')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='',
                                                          awardee_or_recipient_legal='Name')
    # Test ignoring for D records
    det_award_3 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='d',
                                                          awardee_or_recipient_legal=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='D', awardee_or_recipient_legal='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Test fail AwardeeOrRecipientLegalEntityName is required for all submissions except delete records. """

    det_award = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='c', awardee_or_recipient_legal=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr=None,
                                                          awardee_or_recipient_legal='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
