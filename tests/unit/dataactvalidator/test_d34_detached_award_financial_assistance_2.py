from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd34_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "period_of_performance_star", "period_of_performance_curr"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ While they are optional fields, if either PeriodOfPerformanceCurrentEndDate or PeriodOfPerformanceStartDate is
        provided, both fields must be provided. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star=None,
                                                          period_of_performance_curr=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star="20120724",
                                                          period_of_performance_curr="20120724")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 0


def test_failure(database):
    """ While they are optional fields, if either PeriodOfPerformanceCurrentEndDate or PeriodOfPerformanceStartDate is
        provided, both fields must be provided. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star=None,
                                                          period_of_performance_curr="20120724")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star="20120725",
                                                          period_of_performance_curr=None)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
