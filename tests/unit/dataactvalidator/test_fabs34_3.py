from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs34_3"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "period_of_performance_star",
        "period_of_performance_curr",
        "assistance_type",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """PeriodOfPerformanceStartDate and PeriodOfPerformanceCurrentEndDate are required for Grants and Cooperative
    Agreements (AssistanceType = 02, 03, 04, 05, F001, and F002).
    """
    fabs_1 = FABSFactory(
        period_of_performance_star="20120724",
        period_of_performance_curr="20120724",
        assistance_type="02",
        correction_delete_indicatr="c",
    )
    fabs_2 = FABSFactory(
        period_of_performance_star="20120724",
        period_of_performance_curr="20120724",
        assistance_type="F001",
        correction_delete_indicatr="c",
    )
    # Ignore with different assistance type
    fabs_3 = FABSFactory(
        period_of_performance_star="20120724",
        period_of_performance_curr="20120724",
        assistance_type="01",
        correction_delete_indicatr="c",
    )
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(
        period_of_performance_star=None,
        period_of_performance_curr=None,
        assistance_type="03",
        correction_delete_indicatr="d",
    )

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """PeriodOfPerformanceStartDate and PeriodOfPerformanceCurrentEndDate are required for Grants and Cooperative
    Agreements (AssistanceType = 02, 03, 04, 05, F001, and F002).
    """
    fabs_1 = FABSFactory(
        period_of_performance_star="",
        period_of_performance_curr=None,
        assistance_type="03",
        correction_delete_indicatr="c",
    )
    fabs_2 = FABSFactory(
        period_of_performance_star="",
        period_of_performance_curr="20120724",
        assistance_type="04",
        correction_delete_indicatr="c",
    )
    fabs_3 = FABSFactory(
        period_of_performance_star="20120724",
        period_of_performance_curr=None,
        assistance_type="05",
        correction_delete_indicatr="c",
    )
    fabs_4 = FABSFactory(
        period_of_performance_star="20120724",
        period_of_performance_curr=None,
        assistance_type="F002",
        correction_delete_indicatr="c",
    )

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 4
