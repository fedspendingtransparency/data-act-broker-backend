from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from tests.unit.dataactcore.factories.domain import ZipsHistoricalFactory

_FILE = "fabs43_5_3"


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
    """Test if PrimaryPlaceOfPerformanceCongressionalDistrict is provided for an award with a county-wide format
    PrimaryPlaceOfPerformanceCode with an ActionDate before 20230103, then the
    PrimaryPlaceOfPerformanceCongressionalDistrict should be associated with the county embedded in the
    PrimaryPlaceOfPerformanceCode according to the historic USPS source data.
    """
    zips = ZipsHistoricalFactory(county_number="001", state_abbreviation="NY", congressional_district_no="01")

    fabs_1 = FABSFactory(
        place_of_performance_code="NY**001",
        place_of_performance_congr="01",
        correction_delete_indicatr="",
        action_date="20230102",
    )
    fabs_2 = FABSFactory(
        place_of_performance_code="Ny**001",
        place_of_performance_congr="01",
        correction_delete_indicatr=None,
        action_date="20230102",
    )
    # Test ignoring blank/empty string ppop codes
    fabs_3 = FABSFactory(
        place_of_performance_code="",
        place_of_performance_congr="03",
        correction_delete_indicatr="",
        action_date="20230102",
    )
    fabs_4 = FABSFactory(
        place_of_performance_code=None,
        place_of_performance_congr="03",
        correction_delete_indicatr="",
        action_date="20230102",
    )
    # Ignore dates after 20230103
    fabs_5 = FABSFactory(
        place_of_performance_code="nY**001",
        place_of_performance_congr="09",
        correction_delete_indicatr="",
        action_date="20230104",
    )
    # Ignore other ppop code formats
    fabs_6 = FABSFactory(
        place_of_performance_code="nY12001",
        place_of_performance_congr="09",
        correction_delete_indicatr="c",
        action_date="20230102",
    )
    # Ignore CD of 90
    fabs_7 = FABSFactory(
        place_of_performance_code="nY**001",
        place_of_performance_congr="90",
        correction_delete_indicatr="C",
        action_date="20230102",
    )
    # Ignore correction delete indicator of D
    fabs_8 = FABSFactory(
        place_of_performance_code="nY**001",
        place_of_performance_congr="09",
        correction_delete_indicatr="d",
        action_date="20230102",
    )

    errors = number_of_errors(
        _FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7, fabs_8, zips]
    )
    assert errors == 0


def test_failure(database):
    """Test failure if PrimaryPlaceOfPerformanceCongressionalDistrict is provided for an award with a county-wide
    format PrimaryPlaceOfPerformanceCode with an ActionDate before 20230103, then the
    PrimaryPlaceOfPerformanceCongressionalDistrict should be associated with the county embedded in the
    PrimaryPlaceOfPerformanceCode according to the historic USPS source data.
    """
    zips = ZipsHistoricalFactory(county_number="001", state_abbreviation="NY", congressional_district_no="01")

    fabs_1 = FABSFactory(
        place_of_performance_code="NY**001",
        place_of_performance_congr="03",
        correction_delete_indicatr="",
        action_date="20230102",
    )
    fabs_2 = FABSFactory(
        place_of_performance_code="nY**002",
        place_of_performance_congr="01",
        correction_delete_indicatr=None,
        action_date="20230102",
    )
    fabs_3 = FABSFactory(
        place_of_performance_code="pa**001",
        place_of_performance_congr="01",
        correction_delete_indicatr="c",
        action_date="20230102",
    )

    errors = number_of_errors(_FILE, database, models=[fabs_1, fabs_2, fabs_3, zips])
    assert errors == 3
