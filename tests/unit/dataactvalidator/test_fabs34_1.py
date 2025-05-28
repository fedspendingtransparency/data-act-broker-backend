from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs34_1"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "period_of_performance_star",
        "period_of_performance_curr",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """When both are provided, PeriodOfPerformanceStartDate must occur on or before PeriodOfPerformanceCurrentEndDate.
    Null in either doesn't affect success
    """
    fabs_1 = FABSFactory(
        period_of_performance_star="20120724", period_of_performance_curr="20120725", correction_delete_indicatr=""
    )
    fabs_2 = FABSFactory(
        period_of_performance_star="20120724", period_of_performance_curr=None, correction_delete_indicatr=None
    )
    fabs_3 = FABSFactory(
        period_of_performance_star=None, period_of_performance_curr="20120725", correction_delete_indicatr="c"
    )
    fabs_4 = FABSFactory(
        period_of_performance_star=None, period_of_performance_curr=None, correction_delete_indicatr=""
    )
    fabs_5 = FABSFactory(
        period_of_performance_star="20120724", period_of_performance_curr="1234", correction_delete_indicatr=""
    )
    fabs_6 = FABSFactory(
        period_of_performance_star="20120724", period_of_performance_curr="20120724", correction_delete_indicatr="C"
    )
    # Ignore correction delete indicator of D
    fabs_7 = FABSFactory(
        period_of_performance_star="20120725", period_of_performance_curr="20120724", correction_delete_indicatr="d"
    )

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7])
    assert errors == 0


def test_failure(database):
    """When both are provided, PeriodOfPerformanceStartDate must occur on or before PeriodOfPerformanceCurrentEndDate."""
    fabs_1 = FABSFactory(
        period_of_performance_star="20120725", period_of_performance_curr="20120724", correction_delete_indicatr=""
    )

    errors = number_of_errors(_FILE, database, models=[fabs_1])
    assert errors == 1
