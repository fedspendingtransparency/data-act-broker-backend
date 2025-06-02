from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs41_6"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "place_of_performance_code",
        "place_of_performance_zip4a",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """If PrimaryPlaceOfPerformanceCode is XX00000, PrimaryPlaceOfPerformanceZip4 must not be 'city-wide'"""

    fabs_1 = FABSFactory(
        place_of_performance_code="NY00000", place_of_performance_zip4a="", correction_delete_indicatr=""
    )
    fabs_2 = FABSFactory(
        place_of_performance_code="Ny**123", place_of_performance_zip4a="city-wide", correction_delete_indicatr="c"
    )
    fabs_3 = FABSFactory(
        place_of_performance_code="Ny**123", place_of_performance_zip4a="", correction_delete_indicatr=None
    )
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(
        place_of_performance_code="NY00000", place_of_performance_zip4a="city-wide", correction_delete_indicatr="d"
    )
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """Test failure for if PrimaryPlaceOfPerformanceCode is XX00000, PrimaryPlaceOfPerformanceZip4 must
    not be 'city-wide'
    """

    fabs_1 = FABSFactory(
        place_of_performance_code="NY00000", place_of_performance_zip4a="city-wide", correction_delete_indicatr=""
    )
    fabs_2 = FABSFactory(
        place_of_performance_code="va00000", place_of_performance_zip4a="city-wide", correction_delete_indicatr="c"
    )
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 2
