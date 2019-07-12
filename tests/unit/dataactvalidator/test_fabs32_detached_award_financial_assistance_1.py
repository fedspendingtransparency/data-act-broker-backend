from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs32_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'period_of_performance_star'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ PeriodOfPerformanceStartDate is an optional field, but when provided, must follow YYYYMMDD format """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='19990131',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star=None,
                                                          correction_delete_indicatr='c')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='',
                                                          correction_delete_indicatr=None)
    # Ignore correction delete indicator of D
    det_award_4 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='1234',
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ PeriodOfPerformanceStartDate is an optional field, but when provided, must follow YYYYMMDD format """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='19990132',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='19991331',
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='1234',
                                                          correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='200912',
                                                          correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 4
