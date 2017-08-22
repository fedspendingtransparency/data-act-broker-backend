from dataactbroker.handlers.fileHandler import fabs_derivations

from tests.unit.dataactcore.factories.domain import (
    CGACFactory, FRECFactory, SubTierAgencyFactory, StatesFactory, CountyCodeFactory, CFDAProgramFactory,
    ZipCityFactory, ZipsFactory, CityCodeFactory)


def initialize_db_values(db, cfda_title=None, cgac_code=None, frec_code=None, use_frec=False):
    """ Initialize the values in the DB that can be used throughout the tests """
    if cgac_code:
        cgac = CGACFactory(cgac_code=cgac_code, agency_name="Test CGAC Agency")
    else:
        cgac = CGACFactory()
    if frec_code:
        frec = FRECFactory(frec_code=frec_code, agency_name="Test FREC Agency", cgac=cgac)
    else:
        frec = FRECFactory(cgac=cgac)
    db.session.add_all([cgac, frec])
    db.session.commit()

    cfda_number = CFDAProgramFactory(program_number=12.345, program_title=cfda_title)
    sub_tier = SubTierAgencyFactory(sub_tier_agency_code="1234", cgac=cgac, frec=frec, is_frec=use_frec,
                                    sub_tier_agency_name="Test Subtier Agency")
    state = StatesFactory(state_code="NY", state_name="New York")
    zip_code_1 = ZipsFactory(zip5="12345", zip_last4="6789", state_abbreviation=state.state_code, county_number="001",
                             congressional_district_no="01")
    zip_code_2 = ZipsFactory(zip5="12345", zip_last4="4321", state_abbreviation=state.state_code, county_number="001",
                             congressional_district_no="02")
    zip_city = ZipCityFactory(zip_code=zip_code_1.zip5, city_name="Test Zip City")
    county_code = CountyCodeFactory(state_code=state.state_code, county_number=zip_code_1.county_number,
                                    county_name="Test County")
    city_code = CityCodeFactory(feature_name="Test City", city_code="00001", state_code=state.state_code,
                                county_name="Test City County")

    db.session.add_all([sub_tier, state, cfda_number, zip_code_1, zip_code_2, zip_city, county_code, city_code])
    db.session.commit()


def initialize_test_obj(fao=None, nffa=None, cfda_num="00.000", sub_tier_code="1234", fund_agency_code=None,
                        sub_fund_agency_code=None, ppop_code="NY00000", ppop_zip4a=None, ppop_cd=None, le_zip5=None,
                        le_zip4=None, record_type=2, award_mod_amend=None, fain=None, uri=None, cldi=None):
    """ Initialize the values in the object being run through the fabs_derivations function """
    obj = {
        'federal_action_obligation': fao,
        'non_federal_funding_amount': nffa,
        'cfda_number': cfda_num,
        'awarding_sub_tier_agency_c': sub_tier_code,
        'funding_agency_code': fund_agency_code,
        'funding_sub_tier_agency_co': sub_fund_agency_code,
        'place_of_performance_code': ppop_code,
        'place_of_performance_zip4a': ppop_zip4a,
        'place_of_performance_congr': ppop_cd,
        'legal_entity_zip5': le_zip5,
        'legal_entity_zip_last4': le_zip4,
        'record_type': record_type,
        'award_modification_amendme': award_mod_amend,
        'fain': fain,
        'uri': uri,
        'correction_late_delete_ind': cldi
    }
    return obj


def test_total_funding_amount(database):
    initialize_db_values(database)

    # when fao and nffa are empty
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session)
    assert obj['total_funding_amount'] == 0

    # when one of fao and nffa is empty and the other isn't
    obj = initialize_test_obj(fao=5.3)
    obj = fabs_derivations(obj, database.session)
    assert obj['total_funding_amount'] == 5.3

    # when fao and nffa aren't empty
    obj = initialize_test_obj(fao=-10.6, nffa=123)
    obj = fabs_derivations(obj, database.session)
    assert obj['total_funding_amount'] == 112.4


def test_cfda_title(database):
    initialize_db_values(database, cfda_title="Test Title")

    # when cfda_number isn't in the database
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session)
    assert obj['cfda_title'] is None

    # when cfda_number is in the database
    obj = initialize_test_obj(cfda_num="12.345")
    obj = fabs_derivations(obj, database.session)
    assert obj['cfda_title'] == "Test Title"


def test_awarding_agency_cgac(database):
    initialize_db_values(database, cgac_code="000", frec_code="1111")

    obj = initialize_test_obj(sub_tier_code="1234")
    obj = fabs_derivations(obj, database.session)
    assert obj['awarding_agency_code'] == "000"
    assert obj['awarding_agency_name'] == "Test CGAC Agency"
    assert obj['awarding_sub_tier_agency_n'] == "Test Subtier Agency"


def test_awarding_agency_frec(database):
    initialize_db_values(database, cgac_code="000", frec_code="1111", use_frec=True)

    obj = initialize_test_obj(sub_tier_code="1234")
    obj = fabs_derivations(obj, database.session)
    assert obj['awarding_agency_code'] == "1111"
    assert obj['awarding_agency_name'] == "Test FREC Agency"
    assert obj['awarding_sub_tier_agency_n'] == "Test Subtier Agency"


def test_funding_agency_name(database):
    initialize_db_values(database, cgac_code="000")

    # when funding_agency_code is not provided
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session)
    assert obj['funding_agency_name'] is None

    # when funding_agency_code is provided
    obj = initialize_test_obj(fund_agency_code="000")
    obj = fabs_derivations(obj, database.session)
    assert obj['funding_agency_name'] == "Test CGAC Agency"


def test_funding_sub_tier_agency_na(database):
    initialize_db_values(database)

    # when funding_sub_tier_agency_co is not provided
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session)
    assert obj['funding_sub_tier_agency_na'] is None

    # when funding_sub_tier_agency_co is provided
    obj = initialize_test_obj(sub_fund_agency_code="1234")
    obj = fabs_derivations(obj, database.session)
    assert obj['funding_sub_tier_agency_na'] == "Test Subtier Agency"


def test_ppop_state_name(database):
    initialize_db_values(database)

    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session)
    assert obj['place_of_perform_state_nam'] == "New York"


def test_ppop_derivations(database):
    initialize_db_values(database)

    # when ppop_zip4a is 5 digits and no congressional district
    obj = initialize_test_obj(ppop_zip4a="123454321")
    obj = fabs_derivations(obj, database.session)
    assert obj['place_of_performance_congr'] == '02'
    assert obj['place_of_perform_county_na'] == "Test County"
    assert obj['place_of_performance_city'] == "Test Zip City"

    # when ppop_zip4a is 5 digits and has congressional district
    obj = initialize_test_obj(ppop_zip4a="12345-4321", ppop_cd="03")
    obj = fabs_derivations(obj, database.session)
    assert obj['place_of_performance_congr'] == '03'

    # when ppop_zip4a is 4 digits
    obj = initialize_test_obj(ppop_zip4a="12345")
    obj = fabs_derivations(obj, database.session)
    # hard to tell which will be "first" because it depends on what order it's added by sqlalchemy,
    # but we know it has to be one of them
    assert obj['place_of_performance_congr'] == '01' or '02'
    assert obj['place_of_perform_county_na'] == "Test County"
    assert obj['place_of_performance_city'] == "Test Zip City"

    # when we don't have ppop_zip4a and ppop_code is in XX**### format
    obj = initialize_test_obj(ppop_code="NY**001")
    obj = fabs_derivations(obj, database.session)
    assert obj['place_of_perform_county_na'] == "Test County"
    assert obj['place_of_performance_city'] is None
    assert obj['place_of_performance_congr'] is None

    # when we don't have ppop_zip4a and ppop_code is in XX##### format
    obj = initialize_test_obj(ppop_code="NY00001")
    obj = fabs_derivations(obj, database.session)
    assert obj['place_of_perform_county_na'] == "Test City County"
    assert obj['place_of_performance_city'] == "Test City"
    assert obj['place_of_performance_congr'] is None


def test_legal_entity_derivations(database):
    initialize_db_values(database)

    # if there is a legal_entity_zip5, record_type is always 2
    # when we have legal_entity_zip5 and zip4
    obj = initialize_test_obj(le_zip5="12345", le_zip4="6789")
    obj = fabs_derivations(obj, database.session)
    assert obj['legal_entity_city_name'] == "Test Zip City"
    assert obj['legal_entity_congressional'] == "01"
    assert obj['legal_entity_county_code'] == "001"
    assert obj['legal_entity_county_name'] == "Test County"
    assert obj['legal_entity_state_code'] == "NY"
    assert obj['legal_entity_state_name'] == "New York"

    # when we have legal_entity_zip5 but no zip4
    obj = initialize_test_obj(le_zip5="12345", le_zip4="6789")
    obj = fabs_derivations(obj, database.session)
    assert obj['legal_entity_city_name'] == "Test Zip City"
    assert obj['legal_entity_congressional'] == "01" or "02"
    assert obj['legal_entity_county_code'] == "001"
    assert obj['legal_entity_county_name'] == "Test County"
    assert obj['legal_entity_state_code'] == "NY"
    assert obj['legal_entity_state_name'] == "New York"

    # if there is no legal_entity_zip5, record_type is always 1 and ppop_code is always in format XX**###
    obj = initialize_test_obj(record_type=1, ppop_cd="03", ppop_code="NY**001")
    obj = fabs_derivations(obj, database.session)
    assert obj['legal_entity_city_name'] is None
    assert obj['legal_entity_congressional'] == "03"
    assert obj['legal_entity_county_code'] == "001"
    assert obj['legal_entity_county_name'] == "Test County"
    assert obj['legal_entity_state_code'] == "NY"
    assert obj['legal_entity_state_name'] == "New York"


def test_afa_generated_unique(database):
    initialize_db_values(database)

    # Testing with none values
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session)
    assert obj['afa_generated_unique'] == '-none-1234-none--none-'

    # testing with no none values
    obj = initialize_test_obj(award_mod_amend='award', fain='fain', uri='uri')
    obj = fabs_derivations(obj, database.session)
    assert obj['afa_generated_unique'] == 'award1234fainuri'


def test_is_active(database):
    initialize_db_values(database)

    # Testing with none values
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session)
    assert obj['is_active'] is True

    # Testing with value other than D
    obj = initialize_test_obj(cldi="c")
    obj = fabs_derivations(obj, database.session)
    assert obj['is_active'] is True

    # Testing with D
    obj = initialize_test_obj(cldi="D")
    obj = fabs_derivations(obj, database.session)
    assert obj['is_active'] is False
