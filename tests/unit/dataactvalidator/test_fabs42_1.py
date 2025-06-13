from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs42_1"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "place_of_performance_forei",
        "place_of_perform_country_c",
        "record_type",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test PrimaryPlaceOfPerformanceForeignLocationDescription is required for foreign places of performance
     (i.e., when PrimaryPlaceOfPerformanceCountryCode does not equal USA) for record type 2. This test shouldn't
    care about content when country_code is USA (that is for another validation).
    """

    fabs_1 = FABSFactory(
        place_of_performance_forei="description",
        place_of_perform_country_c="UK",
        record_type=2,
        correction_delete_indicatr="",
    )
    fabs_2 = FABSFactory(
        place_of_performance_forei="description",
        place_of_perform_country_c="USA",
        record_type=2,
        correction_delete_indicatr=None,
    )
    fabs_3 = FABSFactory(
        place_of_performance_forei=None,
        place_of_perform_country_c="USA",
        record_type=2,
        correction_delete_indicatr="c",
    )
    fabs_4 = FABSFactory(
        place_of_performance_forei="", place_of_perform_country_c="UsA", record_type=2, correction_delete_indicatr="C"
    )
    fabs_5 = FABSFactory(
        place_of_performance_forei="", place_of_perform_country_c="UK", record_type=1, correction_delete_indicatr=""
    )
    fabs_6 = FABSFactory(
        place_of_performance_forei=None, place_of_perform_country_c="UK", record_type=1, correction_delete_indicatr=""
    )
    # Ignore correction delete indicator of D
    fabs_7 = FABSFactory(
        place_of_performance_forei="", place_of_perform_country_c="UK", record_type=2, correction_delete_indicatr="d"
    )

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7])
    assert errors == 0


def test_failure(database):
    """Test failure PrimaryPlaceOfPerformanceForeignLocationDescription is required for foreign places of performance
    (i.e., when PrimaryPlaceOfPerformanceCountryCode does not equal USA) for record type 2.
    """

    fabs_1 = FABSFactory(
        place_of_performance_forei="", place_of_perform_country_c="UK", record_type=2, correction_delete_indicatr=""
    )
    fabs_2 = FABSFactory(
        place_of_performance_forei=None, place_of_perform_country_c="UK", record_type=2, correction_delete_indicatr="c"
    )

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2])
    assert errors == 2
