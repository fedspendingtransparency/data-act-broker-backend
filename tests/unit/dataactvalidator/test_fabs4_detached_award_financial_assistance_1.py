from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs4_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'action_date', 'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests success for Action date in YYYYMMDD format. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(action_date='19990131', correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    det_award_2 = DetachedAwardFinancialAssistanceFactory(action_date='12345678', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 0


def test_failure(database):
    """ Tests failure for Action date in YYYYMMDD format. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(action_date='19990132', correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(action_date='19991331', correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(action_date=None, correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(action_date="", correction_delete_indicatr='C')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(action_date='200912', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 5
