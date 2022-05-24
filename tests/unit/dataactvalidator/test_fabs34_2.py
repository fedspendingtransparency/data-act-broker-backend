from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs34_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'period_of_performance_star', 'period_of_performance_curr',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ While they are optional fields, if either PeriodOfPerformanceCurrentEndDate or PeriodOfPerformanceStartDate is
        provided, both fields must be provided.
    """
    fabs_1 = FABSFactory(period_of_performance_star=None, period_of_performance_curr=None,
                         correction_delete_indicatr='')
    fabs_2 = FABSFactory(period_of_performance_star='20120724', period_of_performance_curr='20120724',
                         correction_delete_indicatr='c')
    # Ignore correction delete indicator of D
    fabs_3 = FABSFactory(period_of_performance_star=None, period_of_performance_curr='20120724',
                         correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3])
    assert errors == 0


def test_failure(database):
    """ While they are optional fields, if either PeriodOfPerformanceCurrentEndDate or PeriodOfPerformanceStartDate is
        provided, both fields must be provided.
    """
    fabs_1 = FABSFactory(period_of_performance_star=None, period_of_performance_curr='20120724',
                         correction_delete_indicatr='')
    fabs_2 = FABSFactory(period_of_performance_star='20120725', period_of_performance_curr=None,
                         correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 2
