from datetime import date
from dateutil.relativedelta import relativedelta

from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs8_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "fiscal_year_and_quarter_co"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test that first four numbers are correct for current or previous fiscal year """

    today = date.today() + relativedelta(months=3)

    det_award = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co=str(today.year)+"3")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co=str(today.year-1)+"2")
    det_award_null = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co=None)
    det_award_blank = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co="")

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_null, det_award_blank])
    assert errors == 0


def test_failure(database):
    """ Test incorrect dates will fail """

    today = date.today() + relativedelta(months=3)

    det_award_1 = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co=str(today.year+1)+"3")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co=str(today.year-2)+"3")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co="abcde")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co="12342")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 4
