from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabsreq8_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'action_date', 'correction_delete_indicatr'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test ActionDate is required for all submissions except delete records. """

    det_award = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='C', action_date='20171001')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='', action_date='20171001')
    # Test ignoring for D records
    det_award_3 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='d', action_date=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='D', action_date='')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='D', action_date='20171001')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Test fail ActionDate is required for all submissions except delete records. """

    det_award = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='c', action_date=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr=None, action_date='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
