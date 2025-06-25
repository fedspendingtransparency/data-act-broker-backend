from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs33_1"


def test_column_headers(database):
    expected_subset = {"row_number", "period_of_performance_curr", "uniqueid_AssistanceTransactionUniqueKey"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """PeriodOfPerformanceCurrentEndDate is an optional field, but when provided, must follow YYYYMMDD format"""
    fabs_1 = FABSFactory(period_of_performance_curr="19990131", correction_delete_indicatr="")
    fabs_2 = FABSFactory(period_of_performance_curr=None, correction_delete_indicatr="c")
    fabs_3 = FABSFactory(period_of_performance_curr="", correction_delete_indicatr=None)
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(period_of_performance_curr="1234", correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """PeriodOfPerformanceCurrentEndDate is an optional field, but when provided, must follow YYYYMMDD format"""
    fabs_1 = FABSFactory(period_of_performance_curr="19990132", correction_delete_indicatr="")
    fabs_2 = FABSFactory(period_of_performance_curr="19991331", correction_delete_indicatr=None)
    fabs_3 = FABSFactory(period_of_performance_curr="1234", correction_delete_indicatr="c")
    fabs_4 = FABSFactory(period_of_performance_curr="200912", correction_delete_indicatr="C")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 4
