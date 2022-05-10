from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs32_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'period_of_performance_star', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ When provided, PeriodOfPerformanceStartDate must be a valid date between 19991001 and 20991231.
        (i.e., a date between 10/01/1999 and 12/31/2099)
    """
    fabs_1 = FABSFactory(period_of_performance_star='20120725', correction_delete_indicatr='c')
    fabs_2 = FABSFactory(period_of_performance_star=None, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(period_of_performance_star='5', correction_delete_indicatr='')
    fabs_4 = FABSFactory(period_of_performance_star='', correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    fabs_5 = FABSFactory(period_of_performance_star='19990131', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """ When provided, PeriodOfPerformanceStartDate must be a valid date between 19991001 and 20991231.
        (i.e., a date between 10/01/1999 and 12/31/2099)
    """
    fabs_1 = FABSFactory(period_of_performance_star='19990131', correction_delete_indicatr='')
    fabs_2 = FABSFactory(period_of_performance_star='21000101', correction_delete_indicatr='c')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 2
