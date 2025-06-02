from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from dataactcore.models.domainModels import CityCode

_FILE = "fabs41_1"


def test_column_headers(database):
    expected_subset = {"row_number", "place_of_performance_code", "uniqueid_AssistanceTransactionUniqueKey"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """For PrimaryPlaceOfPerformanceCode XX#####, XXTS###, XX####T, or XX####R, where PrimaryPlaceOfPerformanceZIP+4
    is "city-wide": city code #####, TS###, ####T, or ####R must be valid and exist in the provided state.
    """

    city_code = CityCode(city_code="10987", state_code="NY")
    city_code_2 = CityCode(city_code="1098R", state_code="NY")
    city_code_3 = CityCode(city_code="1098T", state_code="NY")
    city_code_4 = CityCode(city_code="TS123", state_code="NY")
    fabs_1 = FABSFactory(
        place_of_performance_code="NY*****", place_of_performance_zip4a="2", correction_delete_indicatr=""
    )
    fabs_2 = FABSFactory(
        place_of_performance_code="NY**123", place_of_performance_zip4a="1", correction_delete_indicatr=None
    )
    fabs_3 = FABSFactory(
        place_of_performance_code="NY**123", place_of_performance_zip4a=None, correction_delete_indicatr="c"
    )
    fabs_4 = FABSFactory(
        place_of_performance_code="ny10986", place_of_performance_zip4a="12345", correction_delete_indicatr="C"
    )
    fabs_5 = FABSFactory(
        place_of_performance_code="Na10987", place_of_performance_zip4a="12345-6789", correction_delete_indicatr=""
    )
    fabs_6 = FABSFactory(
        place_of_performance_code="Ny10987", place_of_performance_zip4a=None, correction_delete_indicatr=""
    )
    fabs_7 = FABSFactory(
        place_of_performance_code="Ny10987", place_of_performance_zip4a="", correction_delete_indicatr=""
    )
    fabs_8 = FABSFactory(
        place_of_performance_code="Ny10987", place_of_performance_zip4a="city-wide", correction_delete_indicatr=""
    )

    # Testing with R ending
    fabs_9 = FABSFactory(
        place_of_performance_code="Ny1098R", place_of_performance_zip4a="city-wide", correction_delete_indicatr=""
    )
    fabs_10 = FABSFactory(
        place_of_performance_code="Ny1098R", place_of_performance_zip4a=None, correction_delete_indicatr="c"
    )

    # Testing with T ending
    fabs_11 = FABSFactory(
        place_of_performance_code="Ny1098t", place_of_performance_zip4a="city-wide", correction_delete_indicatr=""
    )
    fabs_12 = FABSFactory(
        place_of_performance_code="Ny1098T", place_of_performance_zip4a=None, correction_delete_indicatr="c"
    )

    # Testing with TS center
    fabs_13 = FABSFactory(
        place_of_performance_code="Nyts123", place_of_performance_zip4a="city-wide", correction_delete_indicatr=""
    )
    fabs_14 = FABSFactory(
        place_of_performance_code="NyTs123", place_of_performance_zip4a=None, correction_delete_indicatr="c"
    )

    # Ignore correction delete indicator of D
    fabs_15 = FABSFactory(
        place_of_performance_code="ny10986", place_of_performance_zip4a=None, correction_delete_indicatr="d"
    )
    errors = number_of_errors(
        _FILE,
        database,
        models=[
            fabs_1,
            fabs_2,
            fabs_3,
            fabs_4,
            fabs_5,
            fabs_6,
            fabs_7,
            fabs_8,
            fabs_9,
            fabs_10,
            fabs_11,
            fabs_12,
            fabs_13,
            fabs_14,
            fabs_15,
            city_code,
            city_code_2,
            city_code_3,
            city_code_4,
        ],
    )
    assert errors == 0


def test_failure(database):
    """Test failure for PrimaryPlaceOfPerformanceCode XX#####, XXTS###, XX####T, or XX####R, where
    PrimaryPlaceOfPerformanceZIP+4 is "city-wide": city code #####, TS###, ####T, or ####R must be valid and exist
    in the provided state.
    """

    city_code = CityCode(city_code="10987", state_code="NY")
    city_code_2 = CityCode(city_code="1098R", state_code="NY")
    city_code_3 = CityCode(city_code="1098T", state_code="NY")
    city_code_4 = CityCode(city_code="TS123", state_code="NY")
    fabs_1 = FABSFactory(
        place_of_performance_code="ny10986", place_of_performance_zip4a=None, correction_delete_indicatr=None
    )
    fabs_2 = FABSFactory(
        place_of_performance_code="NY10986", place_of_performance_zip4a="", correction_delete_indicatr=""
    )
    fabs_3 = FABSFactory(
        place_of_performance_code="na10987", place_of_performance_zip4a=None, correction_delete_indicatr="c"
    )
    fabs_4 = FABSFactory(
        place_of_performance_code="na1098R", place_of_performance_zip4a=None, correction_delete_indicatr="C"
    )
    fabs_5 = FABSFactory(
        place_of_performance_code="na1098t", place_of_performance_zip4a=None, correction_delete_indicatr="C"
    )
    fabs_6 = FABSFactory(
        place_of_performance_code="naTS123", place_of_performance_zip4a=None, correction_delete_indicatr="C"
    )
    errors = number_of_errors(
        _FILE,
        database,
        models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, city_code, city_code_2, city_code_3, city_code_4],
    )
    assert errors == 6
