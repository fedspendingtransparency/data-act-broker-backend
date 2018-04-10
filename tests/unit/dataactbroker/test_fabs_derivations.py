from dataactbroker.handlers.fabsDerivationsHandler import fabs_derivations
from dataactcore.models.lookups import (ACTION_TYPE_DICT, ASSISTANCE_TYPE_DICT, CORRECTION_DELETE_IND_DICT,
                                        RECORD_TYPE_DICT, BUSINESS_TYPE_DICT, BUSINESS_FUNDS_IND_DICT)

from tests.unit.dataactcore.factories.domain import (
    CGACFactory, FRECFactory, SubTierAgencyFactory, CountyCodeFactory, CFDAProgramFactory, ZipCityFactory, ZipsFactory,
    CityCodeFactory, DunsFactory)

from tests.unit.dataactcore.factories.staging import FPDSContractingOfficeFactory

STATE_DICT = {"NY": "New York"}
COUNTRY_DICT = {"USA": "United States of America"}


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
    zip_code_1 = ZipsFactory(zip5="12345", zip_last4="6789", state_abbreviation="NY", county_number="001",
                             congressional_district_no="01")
    zip_code_2 = ZipsFactory(zip5="12345", zip_last4="4321", state_abbreviation="NY", county_number="001",
                             congressional_district_no="02")
    zip_code_3 = ZipsFactory(zip5="54321", zip_last4="4321", state_abbreviation="NY", county_number="001",
                             congressional_district_no="05")
    zip_code_4 = ZipsFactory(zip5="98765", zip_last4="4321", state_abbreviation="NY", county_number="001",
                             congressional_district_no=None)
    zip_city = ZipCityFactory(zip_code=zip_code_1.zip5, city_name="Test Zip City")
    zip_city_2 = ZipCityFactory(zip_code=zip_code_3.zip5, city_name="Test Zip City 2")
    zip_city_3 = ZipCityFactory(zip_code=zip_code_4.zip5, city_name="Test Zip City 3")
    county_code = CountyCodeFactory(state_code="NY", county_number=zip_code_1.county_number,
                                    county_name="Test County")
    city_code = CityCodeFactory(feature_name="Test City", city_code="00001", state_code="NY",
                                county_number=zip_code_1.county_number, county_name="Test City County")
    contracting_office = FPDSContractingOfficeFactory(contracting_office_code='033103',
                                                      contracting_office_name='Office')
    duns = DunsFactory(awardee_or_recipient_uniqu='123456789', ultimate_parent_unique_ide='234567890',
                       ultimate_parent_legal_enti='Parent 1')
    db.session.add_all([sub_tier, cfda_number, zip_code_1, zip_code_2, zip_code_3, zip_code_4, zip_city, zip_city_2,
                        zip_city_3, county_code, city_code, contracting_office, duns])
    db.session.commit()


def initialize_test_obj(fao=None, nffa=None, cfda_num="00.000", sub_tier_code="1234", sub_fund_agency_code=None,
                        ppop_code="NY00000", ppop_zip4a=None, ppop_cd=None, le_zip5=None, le_zip4=None, record_type=2,
                        award_mod_amend=None, fain=None, uri=None, cdi=None, awarding_office='033103',
                        funding_office='033103', legal_congr=None, legal_city="WASHINGTON", legal_state="DC",
                        primary_place_country='USA', legal_country='USA', detached_award_financial_assistance_id=None,
                        job_id=None, action_type=None, assist_type=None, busi_type=None, busi_fund=None,
                        awardee_or_recipient_uniqu=None
                        ):

    """ Initialize the values in the object being run through the fabs_derivations function """
    obj = {
        'federal_action_obligation': fao,
        'non_federal_funding_amount': nffa,
        'cfda_number': cfda_num,
        'awarding_sub_tier_agency_c': sub_tier_code,
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
        'correction_delete_indicatr': cdi,
        'awarding_office_code': awarding_office,
        'funding_office_code': funding_office,
        'legal_entity_congressional': legal_congr,
        'legal_entity_city_name': legal_city,
        'legal_entity_state_code': legal_state,
        'place_of_perform_country_c': primary_place_country,
        'legal_entity_country_code': legal_country,
        'awardee_or_recipient_uniqu': awardee_or_recipient_uniqu,
        'detached_award_financial_assistance_id': detached_award_financial_assistance_id,
        'job_id': job_id,
        'action_type': action_type,
        'assistance_type': assist_type,
        'business_types': busi_type,
        'business_funds_indicator': busi_fund
    }
    return obj


def test_total_funding_amount(database):
    initialize_db_values(database)

    # when fao and nffa are empty
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['total_funding_amount'] == 0

    # when one of fao and nffa is empty and the other isn't
    obj = initialize_test_obj(fao=5.3)
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['total_funding_amount'] == 5.3

    # when fao and nffa aren't empty
    obj = initialize_test_obj(fao=-10.6, nffa=123)
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['total_funding_amount'] == 112.4


def test_cfda_title(database):
    initialize_db_values(database, cfda_title="Test Title")

    # when cfda_number isn't in the database
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['cfda_title'] is None

    # when cfda_number is in the database
    obj = initialize_test_obj(cfda_num="12.345")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['cfda_title'] == "Test Title"


def test_awarding_agency_cgac(database):
    initialize_db_values(database, cgac_code="000", frec_code="1111")

    obj = initialize_test_obj(sub_tier_code="1234")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['awarding_agency_code'] == "000"
    assert obj['awarding_agency_name'] == "Test CGAC Agency"
    assert obj['awarding_sub_tier_agency_n'] == "Test Subtier Agency"


def test_awarding_agency_frec(database):
    initialize_db_values(database, cgac_code="000", frec_code="1111", use_frec=True)

    obj = initialize_test_obj(sub_tier_code="1234")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['awarding_agency_code'] == "1111"
    assert obj['awarding_agency_name'] == "Test FREC Agency"
    assert obj['awarding_sub_tier_agency_n'] == "Test Subtier Agency"


def test_funding_sub_tier_agency_na(database):
    initialize_db_values(database, cgac_code="5678")

    # when funding_sub_tier_agency_co is not provided
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['funding_sub_tier_agency_na'] is None
    assert obj['funding_agency_code'] is None
    assert obj['funding_agency_name'] is None

    # when funding_sub_tier_agency_co is provided
    obj = initialize_test_obj(sub_fund_agency_code="1234")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['funding_sub_tier_agency_na'] == "Test Subtier Agency"
    assert obj['funding_agency_code'] == '5678'
    assert obj['funding_agency_name'] == 'Test CGAC Agency'


def test_ppop_state(database):
    initialize_db_values(database)

    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_perfor_state_code'] == 'NY'
    assert obj['place_of_perform_state_nam'] == "New York"

    obj = initialize_test_obj(ppop_code="00*****")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_perfor_state_code'] is None
    assert obj['place_of_perform_state_nam'] == "Multi-state"

    obj = initialize_test_obj(ppop_code="", record_type=3)
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_perfor_state_code'] is None
    assert obj['place_of_perform_state_nam'] is None


def test_ppop_derivations(database):
    initialize_db_values(database)

    # when ppop_zip4a is 5 digits and no congressional district
    obj = initialize_test_obj(ppop_zip4a="123454321")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_performance_congr'] == '02'
    assert obj['place_of_perform_county_co'] == "001"
    assert obj['place_of_perform_county_na'] == "Test County"
    assert obj['place_of_performance_city'] == "Test Zip City"

    # when ppop_zip4a is 5 digits and has congressional district
    obj = initialize_test_obj(ppop_zip4a="12345-4321", ppop_cd="03")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_performance_congr'] == '03'

    # when ppop_zip4a is 5 digits
    obj = initialize_test_obj(ppop_zip4a="12345")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    # cd should be 90 if there's more than one option
    assert obj['place_of_performance_congr'] == '90'
    assert obj['place_of_perform_county_co'] == "001"
    assert obj['place_of_perform_county_na'] == "Test County"
    assert obj['place_of_performance_city'] == "Test Zip City"

    # when ppop_zip4 is 9 digits but last 4 are invalid (one cd available)
    obj = initialize_test_obj(ppop_zip4a="543210000")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_performance_congr'] == '05'
    assert obj['place_of_perform_county_co'] == "001"
    assert obj['place_of_perform_county_na'] == "Test County"
    assert obj['place_of_performance_city'] == "Test Zip City 2"

    # when ppop_zip4 is 9 digits (no cd available)
    obj = initialize_test_obj(ppop_zip4a="987654321")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_performance_congr'] == '90'
    assert obj['place_of_perform_county_co'] == "001"
    assert obj['place_of_perform_county_na'] == "Test County"
    assert obj['place_of_performance_city'] == "Test Zip City 3"

    # when we don't have ppop_zip4a and ppop_code is in XX**### format
    obj = initialize_test_obj(ppop_code="NY**001")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_perform_county_co'] == "001"
    assert obj['place_of_perform_county_na'] == "Test County"
    assert obj['place_of_performance_city'] is None
    assert obj['place_of_performance_congr'] is None

    # when we don't have ppop_zip4a and ppop_code is in XX##### format
    obj = initialize_test_obj(ppop_code="NY00001")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_perform_county_co'] == "001"
    assert obj['place_of_perform_county_na'] == "Test City County"
    assert obj['place_of_performance_city'] == "Test City"
    assert obj['place_of_performance_congr'] is None

    # when we don't have a ppop_code at all
    obj = initialize_test_obj(ppop_code="", record_type=3)
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_perform_county_co'] is None
    assert obj['place_of_perform_county_na'] is None
    assert obj['place_of_performance_city'] is None
    assert obj['place_of_performance_congr'] is None


def test_legal_entity_derivations(database):
    initialize_db_values(database)

    # if there is a legal_entity_zip5, record_type is always 2 or 3
    # when we have legal_entity_zip5 and zip4
    obj = initialize_test_obj(le_zip5="12345", le_zip4="6789")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['legal_entity_city_name'] == "Test Zip City"
    assert obj['legal_entity_congressional'] == "01"
    assert obj['legal_entity_county_code'] == "001"
    assert obj['legal_entity_county_name'] == "Test County"
    assert obj['legal_entity_state_code'] == "NY"
    assert obj['legal_entity_state_name'] == "New York"

    # when we have legal_entity_zip5 but no zip4
    obj = initialize_test_obj(le_zip5="12345")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['legal_entity_city_name'] == "Test Zip City"
    # there are multiple options so this should be 90
    assert obj['legal_entity_congressional'] == "90"
    assert obj['legal_entity_county_code'] == "001"
    assert obj['legal_entity_county_name'] == "Test County"
    assert obj['legal_entity_state_code'] == "NY"
    assert obj['legal_entity_state_name'] == "New York"

    # when we have legal_entity_zip5 and congressional but no zip4
    obj = initialize_test_obj(le_zip5="12345", legal_congr="95")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['legal_entity_congressional'] == "95"

    # if there is no legal_entity_zip5 and record_type is 1, ppop_code is always in format XX**###
    obj = initialize_test_obj(record_type=1, ppop_cd="03", ppop_code="NY**001")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['legal_entity_city_name'] is None
    assert obj['legal_entity_congressional'] == "03"
    assert obj['legal_entity_county_code'] == "001"
    assert obj['legal_entity_county_name'] == "Test County"
    assert obj['legal_entity_state_code'] == "NY"
    assert obj['legal_entity_state_name'] == "New York"

    # if there is no legal_entity_zip5, record_type is always 1 and ppop_code can be format XX*****
    obj = initialize_test_obj(record_type=1, ppop_cd=None, ppop_code="NY*****")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['legal_entity_city_name'] is None
    assert obj['legal_entity_congressional'] is None
    assert obj['legal_entity_county_code'] is None
    assert obj['legal_entity_county_name'] is None
    assert obj['legal_entity_state_code'] == "NY"
    assert obj['legal_entity_state_name'] == "New York"

    # if there is no legal_entity_zip5, record_type is always 1 and ppop_code can be format 00FORGN
    obj = initialize_test_obj(record_type=1, ppop_cd=None, ppop_code="00FORGN")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['legal_entity_city_name'] is None
    assert obj['legal_entity_congressional'] is None
    assert obj['legal_entity_county_code'] is None
    assert obj['legal_entity_county_name'] is None
    assert obj['legal_entity_state_code'] is None
    assert obj['legal_entity_state_name'] is None


def test_primary_place_country(database):
    initialize_db_values(database)

    # if primary_plce_of_performance_country_code is present get country name
    obj = initialize_test_obj(primary_place_country='USA')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_perform_country_n'] == 'United States of America'

    obj = initialize_test_obj(primary_place_country='NK')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_perform_country_n'] is None


def test_awarding_office_codes(database):
    initialize_db_values(database)

    # if awarding office_code is present, get office name
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['awarding_office_name'] == 'Office'

    obj = initialize_test_obj(awarding_office='111111')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['awarding_office_name'] is None


def test_funding_office_codes(database):
    initialize_db_values(database)

    # if funding office_code is present, get office name
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['funding_office_name'] == 'Office'

    obj = initialize_test_obj(funding_office='111111')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['funding_office_name'] is None


def test_legal_country(database):
    initialize_db_values(database)

    # if primary_plce_of_performance_country_code is present get country name
    obj = initialize_test_obj(legal_country='USA')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['legal_entity_country_name'] == 'United States of America'

    obj = initialize_test_obj(legal_country='NK')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['legal_entity_country_name'] is None


def test_split_zip(database):
    initialize_db_values(database)

    # testing with 5-digit
    obj = initialize_test_obj(ppop_zip4a='12345')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_performance_zip5'] == '12345'
    assert obj['place_of_perform_zip_last4'] is None

    # testing with 9-digit
    obj = initialize_test_obj(ppop_zip4a='123456789')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_performance_zip5'] == '12345'
    assert obj['place_of_perform_zip_last4'] == '6789'

    # testing with 9-digit and dash
    obj = initialize_test_obj(ppop_zip4a='12345-6789')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_performance_zip5'] == '12345'
    assert obj['place_of_perform_zip_last4'] == '6789'

    # testing with city-wide
    obj = initialize_test_obj(ppop_zip4a='city-wide')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_performance_zip5'] is None
    assert obj['place_of_perform_zip_last4'] is None

    # testing without ppop_zip4
    obj = initialize_test_obj(ppop_zip4a='')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['place_of_performance_zip5'] is None
    assert obj['place_of_perform_zip_last4'] is None


def test_derive_parent_duns(database, monkeypatch):
    initialize_db_values(database)

    obj = initialize_test_obj(awardee_or_recipient_uniqu='123456789')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)

    assert obj['ultimate_parent_legal_enti'] == 'Parent 1'
    assert obj['ultimate_parent_unique_ide'] == '234567890'


def test_derive_parent_duns_return_none(database, monkeypatch):
    initialize_db_values(database)

    obj = initialize_test_obj(awardee_or_recipient_uniqu='123456')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)

    assert not obj['ultimate_parent_legal_enti']
    assert not obj['ultimate_parent_unique_ide']


def test_derive_labels(database):
    initialize_db_values(database)

    # Testing when these values are blank
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['action_type_description'] is None
    assert obj['assistance_type_desc'] is None
    assert obj['correction_delete_ind_desc'] is None
    assert obj['business_types_desc'] is None
    assert obj['business_funds_ind_desc'] is None

    # Testing for valid values of each
    obj = initialize_test_obj(cdi='c', action_type='a', assist_type='02', busi_type='d', busi_fund='non')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['action_type_description'] == ACTION_TYPE_DICT['A']
    assert obj['assistance_type_desc'] == ASSISTANCE_TYPE_DICT['02']
    assert obj['correction_delete_ind_desc'] == CORRECTION_DELETE_IND_DICT['C']
    assert obj['record_type_description'] == RECORD_TYPE_DICT[2]
    assert obj['business_types_desc'] == BUSINESS_TYPE_DICT['D']
    assert obj['business_funds_ind_desc'] == BUSINESS_FUNDS_IND_DICT['NON']

    # Testing for invalid values of each
    obj = initialize_test_obj(cdi='f', action_type='z', assist_type='01', record_type=5, busi_type='Z', busi_fund='ab')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['action_type_description'] is None
    assert obj['assistance_type_desc'] is None
    assert obj['correction_delete_ind_desc'] is None
    assert obj['record_type_description'] is None
    assert obj['business_types_desc'] is None
    assert obj['business_funds_ind_desc'] is None

    # Test multiple business types (2 valid, one invalid)
    obj = initialize_test_obj(cdi='f', action_type='z', assist_type='01', record_type=5, busi_type='azb')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['business_types_desc'] == BUSINESS_TYPE_DICT['A'] + ';' + BUSINESS_TYPE_DICT['B']


def test_is_active(database):
    initialize_db_values(database)

    # Testing with none values
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['is_active'] is True

    # Testing with value other than D
    obj = initialize_test_obj(cdi="c")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['is_active'] is True

    # Testing with D
    obj = initialize_test_obj(cdi="D")
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT)
    assert obj['is_active'] is False
