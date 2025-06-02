from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs39_2"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "record_type",
        "place_of_performance_code",
        "place_of_perform_country_c",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """PrimaryPlaceOfPerformanceCode must be 00FORGN when PrimaryPlaceofPerformanceCountryCode is not USA,
    not 00FORGN otherwise for record type 1 and 2.
    """

    fabs_1 = FABSFactory(
        place_of_performance_code="00FORGN",
        place_of_perform_country_c="UKR",
        record_type=1,
        correction_delete_indicatr="",
    )
    fabs_2 = FABSFactory(
        place_of_performance_code="00FoRGN",
        place_of_perform_country_c="uKr",
        record_type=1,
        correction_delete_indicatr=None,
    )
    fabs_3 = FABSFactory(
        place_of_performance_code="ny**987",
        place_of_perform_country_c="USA",
        record_type=2,
        correction_delete_indicatr="c",
    )
    fabs_4 = FABSFactory(
        place_of_performance_code="NY**987",
        place_of_perform_country_c="UsA",
        record_type=2,
        correction_delete_indicatr="C",
    )
    fabs_5 = FABSFactory(
        place_of_performance_code="NY**987",
        place_of_perform_country_c="UKR",
        record_type=3,
        correction_delete_indicatr="",
    )
    # Ignore correction delete indicator of D
    fabs_6 = FABSFactory(
        place_of_performance_code="00FORGN",
        place_of_perform_country_c="USA",
        record_type=1,
        correction_delete_indicatr="d",
    )
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6])
    assert errors == 0


def test_failure(database):
    """Test failure for PrimaryPlaceOfPerformanceCode must be 00FORGN when PrimaryPlaceofPerformanceCountryCode
    is not USA, not 00FORGN otherwise for record type 1 and 2.
    """

    fabs_1 = FABSFactory(
        place_of_performance_code="00FORGN",
        place_of_perform_country_c="USA",
        record_type=1,
        correction_delete_indicatr="",
    )
    fabs_2 = FABSFactory(
        place_of_performance_code="00FoRGN",
        place_of_perform_country_c="usA",
        record_type=1,
        correction_delete_indicatr=None,
    )
    fabs_3 = FABSFactory(
        place_of_performance_code="ny**987",
        place_of_perform_country_c="UKR",
        record_type=2,
        correction_delete_indicatr="c",
    )
    fabs_4 = FABSFactory(
        place_of_performance_code="NY**987",
        place_of_perform_country_c="ukR",
        record_type=2,
        correction_delete_indicatr="C",
    )
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4])
    assert errors == 4
