from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs34_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'period_of_performance_star', 'period_of_performance_curr',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ When both are provided, PeriodOfPerformanceStartDate must occur on or before PeriodOfPerformanceCurrentEndDate.
        Null in either doesn't affect success
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='20120724',
                                                          period_of_performance_curr='20120725',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='20120724',
                                                          period_of_performance_curr=None,
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star=None,
                                                          period_of_performance_curr='20120725',
                                                          correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star=None,
                                                          period_of_performance_curr=None,
                                                          correction_delete_indicatr='')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='20120724',
                                                          period_of_performance_curr='1234',
                                                          correction_delete_indicatr='')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='20120724',
                                                          period_of_performance_curr='20120724',
                                                          correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    det_award_7 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='20120725',
                                                          period_of_performance_curr='20120724',
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, det_award_7])
    assert errors == 0


def test_failure(database):
    """ When both are provided, PeriodOfPerformanceStartDate must occur on or before PeriodOfPerformanceCurrentEndDate.
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='20120725',
                                                          period_of_performance_curr='20120724',
                                                          correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[det_award_1])
    assert errors == 1
