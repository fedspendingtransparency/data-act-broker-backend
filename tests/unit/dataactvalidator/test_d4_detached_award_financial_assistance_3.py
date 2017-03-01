from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from datetime import date
from dateutil.relativedelta import relativedelta

_FILE = 'd4_detached_award_financial_assistance_3'


def test_column_headers(database):
    expected_subset = {"row_number", "action_date"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests that future ActionDate is valid if it occurs within the current fiscal year """
    today = date.today() + relativedelta(months=1)
    det_award_1 = DetachedAwardFinancialAssistanceFactory(action_date=str(today))
    det_award_2 = DetachedAwardFinancialAssistanceFactory(action_date=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(action_date="5")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ Tests that future ActionDate is invalid if it occurs outside the current fiscal year """
    today = date.today() + relativedelta(years=1)
    det_award_1 = DetachedAwardFinancialAssistanceFactory(action_date=str(today))

    errors = number_of_errors(_FILE, database, models=[det_award_1])
    assert errors == 1
