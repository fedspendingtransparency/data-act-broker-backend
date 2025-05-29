from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = "fabs43_3"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "place_of_performance_congr",
        "record_type",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test PrimaryPlaceOfPerformanceCongressionalDistrict must be blank for PII-redacted non-aggregate records
    (RecordType = 3).
    """
    fabs_1 = FABSFactory(place_of_performance_congr=None, record_type=3, correction_delete_indicatr="")
    fabs_2 = FABSFactory(place_of_performance_congr="", record_type=3, correction_delete_indicatr="c")
    fabs_3 = FABSFactory(place_of_performance_congr="01", record_type=2, correction_delete_indicatr=None)

    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(place_of_performance_congr="01", record_type=3, correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """Test failure PrimaryPlaceOfPerformanceCongressionalDistrict must be blank for PII-redacted non-aggregate records
    (RecordType = 3).
    """

    fabs_1 = FABSFactory(place_of_performance_congr="01", record_type=3, correction_delete_indicatr="")

    errors = number_of_errors(_FILE, database, models=[fabs_1])
    assert errors == 1
