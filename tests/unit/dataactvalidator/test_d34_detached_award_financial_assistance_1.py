from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd34_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {"row_number", "period_of_performance_star", "period_of_performance_curr"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ When both are provided, PeriodOfPerformanceStartDate must occur on or before PeriodOfPerformanceCurrentEndDate.
        Null in either doesn't affect success """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star="20120724",
                                                          period_of_performance_curr="20120725")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star="20120724",
                                                          period_of_performance_curr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star=None,
                                                          period_of_performance_curr="20120725")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star=None,
                                                          period_of_performance_curr=None)
    det_award_5 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star="20120724",
                                                          period_of_performance_curr="1234")
    det_award_6 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star="20120724",
                                                          period_of_performance_curr="20120724")

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6])
    assert errors == 0


def test_failure(database):
    """ When both are provided, PeriodOfPerformanceStartDate must occur on or before PeriodOfPerformanceCurrentEndDate.
        """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star="20120725",
                                                          period_of_performance_curr="20120724")

    errors = number_of_errors(_FILE, database, models=[det_award_1])
    assert errors == 1
