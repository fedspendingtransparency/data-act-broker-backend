from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd8_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "fiscal_year_and_quarter_co"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test that combinations of 4 digits and then a number 1-4 work and that null works """
    det_award = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co="20163")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co="12342")
    det_award_null = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co=None)
    det_award_blank = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co="")

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_null, det_award_blank])
    assert errors == 0


def test_failure(database):
    """ Test that combinations longer than 4 (even if they have things in the right positions), combinations
        shorter than 4, letters, and final digits of 0 or > 4 will fail"""

    det_award_1 = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co="123411")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co="123")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co="abcde")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co="abcde2")
    det_award_5 = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co="2016a")
    det_award_6 = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co="20160")
    det_award_7 = DetachedAwardFinancialAssistanceFactory(fiscal_year_and_quarter_co="20167")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4,
                                                       det_award_5, det_award_6, det_award_7])
    assert errors == 7
