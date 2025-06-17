from tests.unit.dataactcore.factories.staging import FABSFactory
from dataactcore.models.domainModels import CountryCode
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs24_2"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "record_type",
        "place_of_perform_country_c",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """PrimaryPlaceOfPerformanceCountryCode must contain a valid three character GENC country code for aggregate or
    non-aggregate records (RecordType = 1 or 2). U.S. Territories and Freely Associated States must be submitted
    with country code = USA and their state/territory code; they cannot be submitted with their GENC country code.
    """
    cc_1 = CountryCode(country_code="USA", country_name="United States", territory_free_state=False)
    cc_2 = CountryCode(country_code="UKR", country_name="Ukraine", territory_free_state=False)
    fabs = FABSFactory(place_of_perform_country_c="USA", record_type=1, correction_delete_indicatr="")
    fabs_2 = FABSFactory(place_of_perform_country_c="uKr", record_type=2, correction_delete_indicatr="C")
    fabs_3 = FABSFactory(place_of_perform_country_c="abc", record_type=3, correction_delete_indicatr=None)
    # Ignore correction delete indicator of D
    fabs_4 = FABSFactory(place_of_perform_country_c="xyz", record_type=1, correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[cc_1, cc_2, fabs, fabs_2, fabs_3, fabs_4])
    assert errors == 0


def test_failure(database):
    """PrimaryPlaceOfPerformanceCountryCode must contain a valid three character GENC country code for aggregate or
    non-aggregate records (RecordType = 1 or 2). U.S. Territories and Freely Associated States must be submitted
    with country code = USA and their state/territory code; they cannot be submitted with their GENC country code.
    """

    cc_1 = CountryCode(country_code="ASM", country_name="AMERICAN SAMOA", territory_free_state=True)
    fabs = FABSFactory(place_of_perform_country_c="xyz", record_type=1, correction_delete_indicatr="")
    fabs_2 = FABSFactory(place_of_perform_country_c="ABCD", record_type=2, correction_delete_indicatr=None)
    fabs_3 = FABSFactory(place_of_perform_country_c="", record_type=2, correction_delete_indicatr="c")
    fabs_4 = FABSFactory(place_of_perform_country_c="ASM", record_type=1, correction_delete_indicatr="C")

    errors = number_of_errors(_FILE, database, models=[cc_1, fabs, fabs_2, fabs_3, fabs_4])
    assert errors == 4
