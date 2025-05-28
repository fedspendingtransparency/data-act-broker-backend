from tests.unit.dataactcore.factories.staging import FABSFactory
from tests.unit.dataactcore.factories.domain import ZipCityFactory, StateCongressionalFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs44_3_2"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "legal_entity_zip5",
        "legal_entity_congressional",
        "action_date",
        "uniqueid_AssistanceTransactionUniqueKey",
    }
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """Test if LegalEntityCongressionalDistrict is provided, it must be valid in the state or territory indicated by
    LegalEntityZIP5. The LegalEntityCongressionalDistrict may be 90 if the state has more than one congressional
    district.
    """
    zip1 = ZipCityFactory(zip_code="12345", state_code="AB")
    zip2 = ZipCityFactory(zip_code="23456", state_code="CD")
    sc1 = StateCongressionalFactory(state_code="AB", congressional_district_no="01", census_year=None)
    sc2 = StateCongressionalFactory(state_code="CD", congressional_district_no="02", census_year=2020)
    sc3 = StateCongressionalFactory(state_code="CD", congressional_district_no="03", census_year=None)

    fabs_1 = FABSFactory(
        legal_entity_zip5="12345",
        legal_entity_congressional="01",
        correction_delete_indicatr="c",
        action_date="20230104",
    )
    fabs_2 = FABSFactory(
        legal_entity_zip5="23456",
        legal_entity_congressional="02",
        correction_delete_indicatr="",
        action_date="20230104",
    )
    fabs_3 = FABSFactory(
        legal_entity_zip5="23456",
        legal_entity_congressional="90",
        correction_delete_indicatr="",
        action_date="20230104",
    )

    # Test ignore null zips
    fabs_4 = FABSFactory(
        legal_entity_zip5="", legal_entity_congressional="05", correction_delete_indicatr="C", action_date="20230104"
    )

    # Test ignore null CDs
    fabs_5 = FABSFactory(
        legal_entity_zip5="12345", legal_entity_congressional="", correction_delete_indicatr="", action_date="20230104"
    )
    fabs_6 = FABSFactory(
        legal_entity_zip5="", legal_entity_congressional="", correction_delete_indicatr="c", action_date="20230104"
    )
    fabs_7 = FABSFactory(
        legal_entity_zip5="12345",
        legal_entity_congressional=None,
        correction_delete_indicatr=None,
        action_date="20230104",
    )

    # Test ignore action date after before census
    fabs_8 = FABSFactory(
        legal_entity_zip5="12345",
        legal_entity_congressional="05",
        correction_delete_indicatr="C",
        action_date="20230102",
    )

    # Test ignore correction delete indicator of D
    fabs_9 = FABSFactory(
        legal_entity_zip5="12345",
        legal_entity_congressional="03",
        correction_delete_indicatr="d",
        action_date="20230104",
    )

    errors = number_of_errors(
        _FILE,
        database,
        models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, fabs_6, fabs_7, fabs_8, fabs_9, zip1, zip2, sc1, sc2, sc3],
    )
    assert errors == 0


def test_failure(database):
    """Test failure if LegalEntityCongressionalDistrict is provided, it must be valid in the state or territory
    indicated by LegalEntityZIP5. The LegalEntityCongressionalDistrict may be 90 if the state has more than one
    congressional district.
    """
    zip1 = ZipCityFactory(zip_code="12345", state_code="AB")
    zip2 = ZipCityFactory(zip_code="54321", state_code="BC")
    sc1 = StateCongressionalFactory(state_code="AB", congressional_district_no="01", census_year=None)
    sc2 = StateCongressionalFactory(state_code="AB", congressional_district_no="02", census_year=2000)
    sc3 = StateCongressionalFactory(state_code="BC", congressional_district_no="04", census_year=None)

    fabs_1 = FABSFactory(
        legal_entity_zip5="12345",
        legal_entity_congressional="02",
        correction_delete_indicatr="",
        action_date="20230104",
    )
    fabs_2 = FABSFactory(
        legal_entity_zip5="12346",
        legal_entity_congressional="01",
        correction_delete_indicatr=None,
        action_date="20230104",
    )
    fabs_3 = FABSFactory(
        legal_entity_zip5="12345",
        legal_entity_congressional="03",
        correction_delete_indicatr="C",
        action_date="20230104",
    )
    # Entry for a different state doesn't work even if it's a state that exists under a different zip
    fabs_4 = FABSFactory(
        legal_entity_zip5="12345",
        legal_entity_congressional="04",
        correction_delete_indicatr="C",
        action_date="20230104",
    )
    # 90 doesn't work if there's only one CD
    fabs_5 = FABSFactory(
        legal_entity_zip5="54321",
        legal_entity_congressional="90",
        correction_delete_indicatr="C",
        action_date="20230104",
    )

    errors = number_of_errors(
        _FILE, database, models=[fabs_1, fabs_2, fabs_3, fabs_4, fabs_5, zip1, zip2, sc1, sc2, sc3]
    )
    assert errors == 5
