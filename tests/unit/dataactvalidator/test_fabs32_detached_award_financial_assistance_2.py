from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs32_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'period_of_performance_star', 'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ When provided, PeriodOfPerformanceStartDate must be a valid date between 19991001 and 20991231.
        (i.e., a date between 10/01/1999 and 12/31/2099)
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='20120725',
                                                          correction_delete_indicatr='c')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star=None,
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='5', correction_delete_indicatr='')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='', correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    det_award_5 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='19990131',
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ When provided, PeriodOfPerformanceStartDate must be a valid date between 19991001 and 20991231.
        (i.e., a date between 10/01/1999 and 12/31/2099)
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='19990131',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(period_of_performance_star='21000101',
                                                          correction_delete_indicatr='c')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
