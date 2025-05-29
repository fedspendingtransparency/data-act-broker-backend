from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from tests.unit.dataactcore.factories.domain import StateCongressionalFactory

_FILE = "fabs43_4_1"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "place_of_performance_code",
        "place_of_performance_congr",
        "action_date",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test if PrimaryPlaceOfPerformanceCongressionalDistrict is provided, it must be valid in the state or territory
    indicated by the PrimaryPlaceOfPerformanceCode. Data with an ActionDate before 20230103 (the date the 2020
    redistricting took full effect) will be evaluated based on the USPS source data from prior to the 2020
    redistricting. The PrimaryPlaceOfPerformanceCongressionalDistrict may be 90 if the state has more than one
    congressional district or PrimaryPlaceOfPerformanceCode is 00*****.
    """
    state_congr_1 = StateCongressionalFactory(congressional_district_no="01", state_code="NY", census_year=None)
    state_congr_2 = StateCongressionalFactory(congressional_district_no="02", state_code="NY", census_year=None)
    state_congr_3 = StateCongressionalFactory(congressional_district_no="03", state_code="NY", census_year=2000)

    fabs_1 = FABSFactory(
        place_of_performance_code="NY12345",
        place_of_performance_congr="01",
        correction_delete_indicatr="",
        action_date="20230102",
    )
    fabs_2 = FABSFactory(
        place_of_performance_code="ny*****",
        place_of_performance_congr="02",
        correction_delete_indicatr=None,
        action_date="20230102",
    )
    fabs_3 = FABSFactory(
        place_of_performance_code="Ny12345",
        place_of_performance_congr="03",
        correction_delete_indicatr="c",
        action_date="20230102",
    )
    fabs_4 = FABSFactory(
        place_of_performance_code="Ny12345",
        place_of_performance_congr="90",
        correction_delete_indicatr="C",
        action_date="20230102",
    )
    fabs_5 = FABSFactory(
        place_of_performance_code="00*****",
        place_of_performance_congr="90",
        correction_delete_indicatr="",
        action_date="20230102",
    )
    fabs_6 = FABSFactory(
        place_of_performance_code="NY12345",
        place_of_performance_congr="",
        correction_delete_indicatr="",
        action_date="20230102",
    )
    fabs_7 = FABSFactory(
        place_of_performance_code="Ny12345",
        place_of_performance_congr="",
        correction_delete_indicatr="",
        action_date="20230102",
    )
    fabs_8 = FABSFactory(
        place_of_performance_code="Ny12345",
        place_of_performance_congr=None,
        correction_delete_indicatr="",
        action_date="20230102",
    )
    # Test ignoring blank/empty string ppop codes
    fabs_9 = FABSFactory(
        place_of_performance_code="",
        place_of_performance_congr="04",
        correction_delete_indicatr="",
        action_date="20230102",
    )
    fabs_10 = FABSFactory(
        place_of_performance_code=None,
        place_of_performance_congr="04",
        correction_delete_indicatr="",
        action_date="20230102",
    )
    # Test ignoring action dates after census
    fabs_11 = FABSFactory(
        place_of_performance_code="Ny12345",
        place_of_performance_congr="04",
        correction_delete_indicatr="",
        action_date="20230104",
    )
    # Ignore correction delete indicator of D
    fabs_12 = FABSFactory(
        place_of_performance_code="nY12345",
        place_of_performance_congr="09",
        correction_delete_indicatr="d",
        action_date="20230102",
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
            state_congr_1,
            state_congr_2,
            state_congr_3,
        ],
    )
    assert errors == 0


def test_failure(database):
    """Test failure if PrimaryPlaceOfPerformanceCongressionalDistrict is provided, it must be valid in the state or
    territory indicated by the PrimaryPlaceOfPerformanceCode. Data with an ActionDate before 20230103 (the date the
    2020 redistricting took full effect) will be evaluated based on the USPS source data from prior to the 2020
    redistricting. The PrimaryPlaceOfPerformanceCongressionalDistrict may be 90 if the state has more than one
    congressional district or PrimaryPlaceOfPerformanceCode is 00*****.
    """
    state_congr_1 = StateCongressionalFactory(congressional_district_no="01", state_code="NY", census_year=None)
    state_congr_2 = StateCongressionalFactory(congressional_district_no="02", state_code="NY", census_year=None)
    state_congr_3 = StateCongressionalFactory(congressional_district_no="01", state_code="PA", census_year=None)
    state_congr_4 = StateCongressionalFactory(congressional_district_no="03", state_code="NJ", census_year=2020)

    fabs_1 = FABSFactory(
        place_of_performance_code="nY12345",
        place_of_performance_congr="03",
        correction_delete_indicatr="",
        action_date="20230102",
    )
    fabs_2 = FABSFactory(
        place_of_performance_code="PA12345",
        place_of_performance_congr="02",
        correction_delete_indicatr=None,
        action_date="20230102",
    )
    fabs_3 = FABSFactory(
        place_of_performance_code="PA**345",
        place_of_performance_congr="90",
        correction_delete_indicatr="c",
        action_date="20230102",
    )
    fabs_4 = FABSFactory(
        place_of_performance_code="00*****",
        place_of_performance_congr="01",
        correction_delete_indicatr="C",
        action_date="20230102",
    )
    fabs_5 = FABSFactory(
        place_of_performance_code="NJ12345",
        place_of_performance_congr="03",
        correction_delete_indicatr="",
        action_date="20230102",
    )

    errors = number_of_errors(
        _FILE,
        database,
        models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, state_congr_1, state_congr_2, state_congr_3, state_congr_4],
    )
    assert errors == 5
