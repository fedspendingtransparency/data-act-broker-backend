from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd4_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "action_date"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests success for Action date in YYYYMMDD format """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(action_date="19990131")

    errors = number_of_errors(_FILE, database, models=[det_award_1])
    assert errors == 0


def test_failure(database):
    """ Tests failure for Action date in YYYYMMDD format """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(action_date="19990132")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(action_date="19991331")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(action_date=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(action_date="")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(action_date="200912")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 5
