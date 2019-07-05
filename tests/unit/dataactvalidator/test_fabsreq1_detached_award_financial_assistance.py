from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabsreq1_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'award_description', 'correction_delete_indicatr'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test AwardDescription is required for all submissions except delete records. """

    det_award = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='C', award_description='Yes')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='', award_description='No')
    # Test ignoring for D records
    det_award_3 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='d', award_description=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='D', award_description='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Test fail AwardDescription is required for all submissions except delete records. """

    det_award = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='c', award_description=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr=None, award_description='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
