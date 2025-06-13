from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs41_7"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "record_type",
        "place_of_performance_zip4a",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """PrimaryPlaceOfPerformanceZip+4 must be blank for PII-redacted non-aggregate records (i.e., RecordType = 3)."""

    fabs_1 = FABSFactory(place_of_performance_zip4a="abc", record_type=1, correction_delete_indicatr="")
    fabs_2 = FABSFactory(place_of_performance_zip4a="123", record_type=2, correction_delete_indicatr="c")
    fabs_3 = FABSFactory(place_of_performance_zip4a="", record_type=3, correction_delete_indicatr=None)
    fabs_4 = FABSFactory(place_of_performance_zip4a=None, record_type=3, correction_delete_indicatr="C")
    # Ignore correction delete indicator of D
    fabs_5 = FABSFactory(place_of_performance_zip4a="12345", record_type=3, correction_delete_indicatr="d")
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """Test failure for PrimaryPlaceOfPerformanceZip+4 must be blank for PII-redacted non-aggregate records
    (i.e., RecordType = 3).
    """

    fabs_1 = FABSFactory(place_of_performance_zip4a="12345", record_type=3, correction_delete_indicatr="")
    errors = number_of_errors(_FILE, database, models=[fabs_1])
    assert errors == 1
