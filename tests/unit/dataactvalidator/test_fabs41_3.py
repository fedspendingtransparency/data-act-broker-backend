from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from dataactcore.models.domainModels import Zips

_FILE = "fabs41_3"


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
    """When provided, PrimaryPlaceofPerformanceZIP+4 must be in the state specified by PrimaryPlaceOfPerformanceCode.
    In this specific submission row, the ZIP5 (and by extension the full ZIP+4) is not a valid ZIP code in the
    state in question.
    """

    zips = Zips(zip5="12345", zip_last4="6789", state_abbreviation="NY")
    # ignored because no zip4
    fabs_1 = FABSFactory(
        place_of_performance_code="NY*****", place_of_performance_zip4a="", correction_delete_indicatr=""
    )
    fabs_2 = FABSFactory(
        place_of_performance_code="Ny**123", place_of_performance_zip4a=None, correction_delete_indicatr=None
    )
    fabs_3 = FABSFactory(
        place_of_performance_code="Ny**123", place_of_performance_zip4a="city-wide", correction_delete_indicatr=""
    )

    # valid 5 digit zip
    fabs_4 = FABSFactory(
        place_of_performance_code="Ny**123", place_of_performance_zip4a="12345", correction_delete_indicatr="c"
    )
    fabs_5 = FABSFactory(
        place_of_performance_code="NY98765", place_of_performance_zip4a="12345", correction_delete_indicatr=""
    )

    # valid 9 digit zip
    fabs_6 = FABSFactory(
        place_of_performance_code="NY98765", place_of_performance_zip4a="123456789", correction_delete_indicatr="C"
    )
    fabs_7 = FABSFactory(
        place_of_performance_code="ny98765", place_of_performance_zip4a="123456789", correction_delete_indicatr=""
    )
    fabs_8 = FABSFactory(
        place_of_performance_code="ny98765", place_of_performance_zip4a="12345-6789", correction_delete_indicatr=""
    )

    # invalid 9 digit zip but this should pass for this rule, it will be handled for d_41_5
    fabs_9 = FABSFactory(
        place_of_performance_code="ny98765", place_of_performance_zip4a="12345-6788", correction_delete_indicatr=""
    )

    # Ignore correction delete indicator of D
    fabs_10 = FABSFactory(
        place_of_performance_code="ny10986", place_of_performance_zip4a="12346", correction_delete_indicatr="d"
    )
    errors = number_of_errors(
        _FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7, fabs_8, fabs_9, fabs_10, zips]
    )
    assert errors == 0

    # random wrong length zips and zips with '-' in the wrong place, formatting is checked in another rule
    fabs_1 = FABSFactory(
        place_of_performance_code="ny10986", place_of_performance_zip4a="12345678", correction_delete_indicatr=""
    )
    fabs_2 = FABSFactory(
        place_of_performance_code="ny10986", place_of_performance_zip4a="1234567898", correction_delete_indicatr=""
    )
    fabs_3 = FABSFactory(
        place_of_performance_code="ny10986", place_of_performance_zip4a="12345678-9", correction_delete_indicatr=""
    )
    fabs_4 = FABSFactory(
        place_of_performance_code="ny10986", place_of_performance_zip4a="123-456789", correction_delete_indicatr=""
    )
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, zips])
    assert errors == 0


def test_failure(database):
    """Test failure for when provided, PrimaryPlaceofPerformanceZIP+4 must be in the state specified by
    PrimaryPlaceOfPerformanceCode. In this specific submission row, the ZIP5 (and by extension the full ZIP+4) is
    not a valid ZIP code in the state in question.
    """

    zips = Zips(zip5="12345", zip_last4="6789", state_abbreviation="NY")
    # invalid 5 digit zip
    fabs_1 = FABSFactory(
        place_of_performance_code="ny10986", place_of_performance_zip4a="12346", correction_delete_indicatr=""
    )
    fabs_2 = FABSFactory(
        place_of_performance_code="NA*****", place_of_performance_zip4a="12345", correction_delete_indicatr="c"
    )
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, zips])
    assert errors == 2

    # invalid 9 digit zip - first five fail (see d41_5 for the last four to fail)
    fabs_1 = FABSFactory(
        place_of_performance_code="ny10986", place_of_performance_zip4a="123466789", correction_delete_indicatr=None
    )
    fabs_2 = FABSFactory(
        place_of_performance_code="NY*****", place_of_performance_zip4a="12346-6789", correction_delete_indicatr="C"
    )
    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, zips])
    assert errors == 2
