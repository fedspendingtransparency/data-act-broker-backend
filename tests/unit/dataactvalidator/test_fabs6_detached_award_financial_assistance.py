from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs6_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'record_type', 'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests success for when Record type is required and cannot be blank. It must be 1, 2, or 3 """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(record_type=1, correction_delete_indicatr=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=2, correction_delete_indicatr='')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(record_type=3, correction_delete_indicatr='c')
    # Ignore correction delete indicator of D
    det_award_4 = DetachedAwardFinancialAssistanceFactory(record_type=0, correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Tests failure for when Record type is required and cannot be blank. It must be 1, 2, or 3 """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(record_type=0, correction_delete_indicatr=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(record_type=None, correction_delete_indicatr='c')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
