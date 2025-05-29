from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs43_1"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "place_of_perform_country_c",
        "place_of_performance_congr",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """If PrimaryPlaceOfPerformanceCode is not USA, Congressional District must be blank"""

    fabs_1 = FABSFactory(place_of_perform_country_c="Nk", place_of_performance_congr="", correction_delete_indicatr="")
    fabs_2 = FABSFactory(
        place_of_perform_country_c="CA", place_of_performance_congr=None, correction_delete_indicatr="c"
    )
    fabs_3 = FABSFactory(
        place_of_perform_country_c="Usa", place_of_performance_congr="", correction_delete_indicatr=None
    )
    fabs_4 = FABSFactory(
        place_of_perform_country_c="USA", place_of_performance_congr="12", correction_delete_indicatr="C"
    )
    # Ignore correction delete indicator of D
    fabs_5 = FABSFactory(
        place_of_perform_country_c="Nk", place_of_performance_congr="12", correction_delete_indicatr="d"
    )
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """Test failure for if PrimaryPlaceOfPerformanceCode is not USA, Congressional District must be blank"""

    fabs_1 = FABSFactory(
        place_of_perform_country_c="Nk", place_of_performance_congr="12", correction_delete_indicatr=""
    )
    fabs_2 = FABSFactory(
        place_of_perform_country_c="CA", place_of_performance_congr="32", correction_delete_indicatr=None
    )
    fabs_3 = FABSFactory(
        place_of_perform_country_c="Mx", place_of_performance_congr="09", correction_delete_indicatr="c"
    )
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3])
    assert errors == 3
