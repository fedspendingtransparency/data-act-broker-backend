from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd4_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "action_date"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests if value of action date is be between 19991001 and 20991231
        (i.e., a date between 10/01/1999 and 12/31/2099) """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(action_date="20120725")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(action_date=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(action_date="5")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ Tests if value of action date is not be between 19991001 and 20991231
        (i.e., a date between 10/01/1999 and 12/31/2099) """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(action_date="19990131")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(action_date="21000101")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
