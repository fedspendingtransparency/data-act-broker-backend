from dataactbroker.helpers.fabs_derivations_helper import fabs_derivations
from dataactcore.models.lookups import (ACTION_TYPE_DICT, ASSISTANCE_TYPE_DICT, CORRECTION_DELETE_IND_DICT,
                                        RECORD_TYPE_DICT, BUSINESS_TYPE_DICT, BUSINESS_FUNDS_IND_DICT)

from tests.unit.dataactcore.factories.domain import ZipCityFactory, ZipsFactory, CityCodeFactory, DunsFactory
from tests.unit.dataactcore.factories.staging import PublishedAwardFinancialAssistanceFactory

STATE_DICT = {'NY': 'New York'}
COUNTRY_DICT = {'USA': 'United States of America', 'GBR': 'Great Britain'}
SUB_TIER_DICT = {
    '12AB': {
        'is_frec': False,
        'cgac_code': '000',
        'frec_code': '0000',
        'sub_tier_agency_name': 'Test Subtier Agency',
        'agency_name': 'Test CGAC Agency'
    },
    '4321': {
        'is_frec': True,
        'cgac_code': '111',
        'frec_code': '1111',
        'sub_tier_agency_name': 'Test Frec Subtier Agency',
        'agency_name': 'Test FREC Agency'
    }
}
CFDA_DICT = {'12.345': 'CFDA Title'}
COUNTY_DICT = {'NY001': 'Test County'}
OFFICE_DICT = {'03AB03': {'office_name': 'Office',
                          'sub_tier_code': '12Ab',
                          'agency_code': '000',
                          'financial_assistance_awards_office': True,
                          'funding_office': True},
               '654321': {'office_name': 'Office',
                          'sub_tier_code': '12Ab',
                          'agency_code': '000',
                          'financial_assistance_awards_office': False,
                          'funding_office': False}}

EXEC_COMP_DICT = {'123456789': {'officer1_name': 'Officer 1',
                                'officer1_amt': '15',
                                'officer2_name': 'Officer 2',
                                'officer2_amt': '77.12',
                                'officer3_name': 'This is the third Officer',
                                'officer3_amt': None,
                                'officer4_name': None,
                                'officer4_amt': '0',
                                'officer5_name': None,
                                'officer5_amt': None}}


def initialize_db_values(db):
    """ Initialize the values in the DB that can be used throughout the tests """
    zip_code_1 = ZipsFactory(zip5='12345', zip_last4='6789', state_abbreviation='NY', county_number='001',
                             congressional_district_no='01')
    zip_code_2 = ZipsFactory(zip5='12345', zip_last4='4321', state_abbreviation='NY', county_number='001',
                             congressional_district_no='02')
    zip_code_3 = ZipsFactory(zip5='54321', zip_last4='4321', state_abbreviation='NY', county_number='001',
                             congressional_district_no='05')
    zip_code_4 = ZipsFactory(zip5='98765', zip_last4='4321', state_abbreviation='NY', county_number='001',
                             congressional_district_no=None)
    zip_city = ZipCityFactory(zip_code=zip_code_1.zip5, city_name='Test City')
    zip_city_2 = ZipCityFactory(zip_code=zip_code_3.zip5, city_name='Test City 2')
    zip_city_3 = ZipCityFactory(zip_code=zip_code_4.zip5, city_name='Test City 3')
    city_code = CityCodeFactory(feature_name='Test City', city_code='00001', state_code='NY',
                                county_number=zip_code_1.county_number, county_name='Test City County')
    duns_1 = DunsFactory(awardee_or_recipient_uniqu='123456789', ultimate_parent_unique_ide='234567890',
                         ultimate_parent_legal_enti='Parent 1')
    duns_2a = DunsFactory(awardee_or_recipient_uniqu='234567890', ultimate_parent_unique_ide='234567890',
                          ultimate_parent_legal_enti='Parent 2')
    duns_2b = DunsFactory(awardee_or_recipient_uniqu='234567890', ultimate_parent_unique_ide=None,
                          ultimate_parent_legal_enti=None)
    duns_3 = DunsFactory(awardee_or_recipient_uniqu='345678901', ultimate_parent_unique_ide=None,
                         ultimate_parent_legal_enti=None)
    # record type 2 pafas
    pafa_1 = PublishedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='12aB', fain='12345', uri='123456',
                                                      action_date='04/28/2000', funding_office_code=None,
                                                      awarding_office_code='03aB03', is_active=True, record_type=2,
                                                      award_modification_amendme='0')
    pafa_2 = PublishedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='12aB', fain='123456', uri='1234567',
                                                      action_date='04/28/2000', funding_office_code='03aB03',
                                                      awarding_office_code=None, is_active=True, record_type=2,
                                                      award_modification_amendme=None)
    # record type 1 pafas
    pafa_3 = PublishedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='12aB', fain='54321', uri='654321',
                                                      action_date='04/28/2000', funding_office_code=None,
                                                      awarding_office_code='03aB03', is_active=True, record_type=1,
                                                      award_modification_amendme=None)
    pafa_4 = PublishedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='12aB', fain='654321', uri='7654321',
                                                      action_date='04/28/2000', funding_office_code='03aB03',
                                                      awarding_office_code=None, is_active=True, record_type=1,
                                                      award_modification_amendme='0')
    # record type 1 base pafa with invalid office codes
    pafa_5 = PublishedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='12aB', fain='abcd', uri='efg',
                                                      action_date='04/28/2000', funding_office_code='123456',
                                                      awarding_office_code='123456', is_active=True, record_type=1,
                                                      award_modification_amendme='0')
    # record type 1 base pafa with valid office codes but they aren't grant or funding type
    pafa_6 = PublishedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='12aB', fain='efg', uri='abcd',
                                                      action_date='04/28/2000', funding_office_code='654321',
                                                      awarding_office_code='654321', is_active=True, record_type=1,
                                                      award_modification_amendme='0')
    db.session.add_all([zip_code_1, zip_code_2, zip_code_3, zip_code_4, zip_city, zip_city_2, zip_city_3, city_code,
                        duns_1, duns_2a, duns_2b, duns_3, pafa_1, pafa_2, pafa_3, pafa_4, pafa_5, pafa_6])
    db.session.commit()


def initialize_test_obj(fao=None, nffa=None, cfda_num='00.000', sub_tier_code='12aB', sub_fund_agency_code=None,
                        ppop_code='NY00000', ppop_zip4a=None, ppop_cd=None, le_zip5=None, le_zip4=None, record_type=2,
                        award_mod_amend=None, fain=None, uri=None, cdi=None, awarding_office='03ab03',
                        funding_office='03ab03', legal_congr=None, legal_city='WASHINGTON', primary_place_country='USA',
                        legal_country='USA', legal_foreign_city=None, detached_award_financial_assistance_id=None,
                        job_id=None, action_type=None, assist_type=None, busi_type=None, busi_fund=None,
                        awardee_or_recipient_uniqu=None):
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
        'place_of_perform_country_c': primary_place_country,
        'legal_entity_country_code': legal_country,
        'legal_entity_foreign_city': legal_foreign_city,
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
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['total_funding_amount'] == 0

    # when one of fao and nffa is empty and the other isn't
    obj = initialize_test_obj(fao=5.3)
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['total_funding_amount'] == 5.3

    # when fao and nffa aren't empty
    obj = initialize_test_obj(fao=-10.6, nffa=123)
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['total_funding_amount'] == 112.4


def test_cfda_title(database):
    initialize_db_values(database)

    # when cfda_number isn't in the database
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['cfda_title'] is None

    # when cfda_number is in the database
    obj = initialize_test_obj(cfda_num='12.345')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['cfda_title'] == 'CFDA Title'


def test_awarding_agency_cgac(database):
    initialize_db_values(database)

    obj = initialize_test_obj(sub_tier_code='12ab')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_agency_code'] == '000'
    assert obj['awarding_agency_name'] == 'Test CGAC Agency'
    assert obj['awarding_sub_tier_agency_n'] == 'Test Subtier Agency'


def test_awarding_agency_frec(database):
    initialize_db_values(database)

    obj = initialize_test_obj(sub_tier_code='4321')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_agency_code'] == '1111'
    assert obj['awarding_agency_name'] == 'Test FREC Agency'
    assert obj['awarding_sub_tier_agency_n'] == 'Test Frec Subtier Agency'


def test_funding_sub_tier_agency_na(database):
    initialize_db_values(database)

    # when funding_sub_tier_agency_co is not provided, it should get derived
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['funding_sub_tier_agency_na'] == 'Test Subtier Agency'
    assert obj['funding_agency_code'] == '000'
    assert obj['funding_agency_name'] == 'Test CGAC Agency'

    # when funding_sub_tier_agency_co is provided
    obj = initialize_test_obj(sub_fund_agency_code='4321')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['funding_sub_tier_agency_na'] == 'Test Frec Subtier Agency'
    assert obj['funding_agency_code'] == '1111'
    assert obj['funding_agency_name'] == 'Test FREC Agency'


def test_ppop_state(database):
    initialize_db_values(database)

    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_perfor_state_code'] == 'NY'
    assert obj['place_of_perform_state_nam'] == 'New York'

    obj = initialize_test_obj(ppop_code='00*****')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_perfor_state_code'] is None
    assert obj['place_of_perform_state_nam'] == 'Multi-state'

    obj = initialize_test_obj(ppop_code='', record_type=3)
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_perfor_state_code'] is None
    assert obj['place_of_perform_state_nam'] is None


def test_ppop_derivations(database):
    initialize_db_values(database)

    # when ppop_zip4a is 5 digits and no congressional district
    obj = initialize_test_obj(ppop_zip4a='123454321', ppop_code='NY00001')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_congr'] == '02'
    assert obj['place_of_perform_county_co'] == '001'
    assert obj['place_of_perform_county_na'] == 'Test County'
    assert obj['place_of_performance_city'] == 'Test City'

    # when ppop_zip4a is 5 digits and has congressional district
    obj = initialize_test_obj(ppop_zip4a='12345-4321', ppop_cd='03', ppop_code='NY0000r')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_congr'] == '03'

    # when ppop_zip4a is 5 digits
    obj = initialize_test_obj(ppop_zip4a='12345')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    # cd should be 90 if there's more than one option
    assert obj['place_of_performance_congr'] == '90'
    assert obj['place_of_perform_county_co'] == '001'
    assert obj['place_of_perform_county_na'] == 'Test County'
    assert obj['place_of_performance_city'] == 'Test City'

    # when ppop_zip4 is 9 digits but last 4 are invalid (one cd available)
    obj = initialize_test_obj(ppop_zip4a='543210000', ppop_code=None)
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_congr'] == '05'
    assert obj['place_of_perform_county_co'] == '001'
    assert obj['place_of_perform_county_na'] == 'Test County'
    assert obj['place_of_performance_city'] == 'Test City 2'

    # when ppop_zip4 is 9 digits (no cd available)
    obj = initialize_test_obj(ppop_zip4a='987654321', ppop_code='NY0001r')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_congr'] == '90'
    assert obj['place_of_perform_county_co'] == '001'
    assert obj['place_of_perform_county_na'] == 'Test County'
    assert obj['place_of_performance_city'] == 'Test City 3'

    # when ppop_zip4 is 'city-wide'
    obj = initialize_test_obj(ppop_zip4a='City-WIDE', ppop_code='NY0001R')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)

    # when we don't have ppop_zip4a and ppop_code is in XX**### format
    obj = initialize_test_obj(ppop_code='Ny**001')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_perform_county_co'] == '001'
    assert obj['place_of_perform_county_na'] == 'Test County'
    assert obj['place_of_performance_city'] is None
    assert obj['place_of_performance_congr'] is None

    # when we don't have ppop_zip4a and ppop_code is in XX##### format
    obj = initialize_test_obj(ppop_code='Ny00001')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_perform_county_co'] == '001'
    assert obj['place_of_perform_county_na'] == 'Test City County'
    assert obj['place_of_performance_city'] == 'Test City'
    assert obj['place_of_performance_congr'] is None

    # when we don't have a ppop_code at all
    obj = initialize_test_obj(ppop_code='', record_type=3)
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_perform_county_co'] is None
    assert obj['place_of_perform_county_na'] is None
    assert obj['place_of_performance_city'] is None
    assert obj['place_of_performance_congr'] is None


def test_legal_entity_derivations(database):
    initialize_db_values(database)

    # if there is a legal_entity_zip5, record_type is always 2 or 3
    # when we have legal_entity_zip5 and zip4
    obj = initialize_test_obj(le_zip5='12345', le_zip4='6789')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['legal_entity_city_name'] == 'Test City'
    assert obj['legal_entity_congressional'] == '01'
    assert obj['legal_entity_county_code'] == '001'
    assert obj['legal_entity_county_name'] == 'Test County'
    assert obj['legal_entity_state_code'] == 'NY'
    assert obj['legal_entity_state_name'] == 'New York'

    # when we have legal_entity_zip5 but no zip4
    obj = initialize_test_obj(le_zip5='12345')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['legal_entity_city_name'] == 'Test City'
    # there are multiple options so this should be 90
    assert obj['legal_entity_congressional'] == '90'
    assert obj['legal_entity_county_code'] == '001'
    assert obj['legal_entity_county_name'] == 'Test County'
    assert obj['legal_entity_state_code'] == 'NY'
    assert obj['legal_entity_state_name'] == 'New York'

    # when we have legal_entity_zip5 and congressional but no zip4
    obj = initialize_test_obj(le_zip5='12345', legal_congr='95')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['legal_entity_congressional'] == '95'

    # if there is no legal_entity_zip5 and record_type is 1, ppop_code is always in format XX**###
    obj = initialize_test_obj(record_type=1, ppop_cd='03', ppop_code='NY**001')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['legal_entity_city_name'] is None
    assert obj['legal_entity_congressional'] == '03'
    assert obj['legal_entity_county_code'] == '001'
    assert obj['legal_entity_county_name'] == 'Test County'
    assert obj['legal_entity_state_code'] == 'NY'
    assert obj['legal_entity_state_name'] == 'New York'

    # if there is no legal_entity_zip5, record_type is always 1 and ppop_code can be format XX*****
    obj = initialize_test_obj(record_type=1, ppop_cd=None, ppop_code='NY*****')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['legal_entity_city_name'] is None
    assert obj['legal_entity_congressional'] is None
    assert obj['legal_entity_county_code'] is None
    assert obj['legal_entity_county_name'] is None
    assert obj['legal_entity_state_code'] == 'NY'
    assert obj['legal_entity_state_name'] == 'New York'

    # if there is no legal_entity_zip5, record_type is always 1 and ppop_code can be format 00FORGN
    obj = initialize_test_obj(record_type=1, ppop_cd=None, ppop_code='00FORGN')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
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
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_perform_country_n'] == 'United States of America'

    obj = initialize_test_obj(primary_place_country='NK')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_perform_country_n'] is None


def test_derive_office_data(database):
    initialize_db_values(database)

    # if office_code is present, get office name
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_office_name'] == 'Office'
    assert obj['funding_office_name'] == 'Office'

    # if office_code is not present, derive it from historical data (record type 2 or 3 uses fain, ignores uri)
    # In this case, there is no funding office but there is an awarding office
    obj = initialize_test_obj(awarding_office=None, funding_office=None, fain='12345', award_mod_amend='1')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_office_code'] == '03aB03'
    assert obj['awarding_office_name'] == 'Office'
    assert obj['funding_office_code'] is None
    assert obj['funding_office_name'] is None

    # if office_code is not present, and no valid fain is given, office code and name are blank
    obj = initialize_test_obj(awarding_office=None, funding_office=None, fain='54321', award_mod_amend='1')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_office_code'] is None
    assert obj['awarding_office_name'] is None
    assert obj['funding_office_code'] is None
    assert obj['funding_office_name'] is None

    # if office_code is not present, and valid fain is given, with no office codes, office code and name are blank
    # In this case, funding office is present, awarding office is not
    obj = initialize_test_obj(awarding_office=None, funding_office=None, fain='123456', award_mod_amend='1')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_office_code'] is None
    assert obj['awarding_office_name'] is None
    assert obj['funding_office_code'] == '03aB03'
    assert obj['funding_office_name'] == 'Office'

    # if office_code is not present, derive it from historical data (record type 1 uses uri, ignores fain)
    # In this case, awarding office is present, funding office is not
    obj = initialize_test_obj(awarding_office=None, funding_office=None, uri='654321', record_type=1,
                              award_mod_amend='1')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_office_code'] == '03aB03'
    assert obj['awarding_office_name'] == 'Office'
    assert obj['funding_office_code'] is None
    assert obj['funding_office_name'] is None

    # if office_code is not present, and no valid uri is given, office code and name are blank
    obj = initialize_test_obj(awarding_office=None, funding_office=None, uri='54321', record_type=1,
                              award_mod_amend='1')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_office_code'] is None
    assert obj['awarding_office_name'] is None
    assert obj['funding_office_code'] is None
    assert obj['funding_office_name'] is None

    # if office_code is not present, and valid uri is given, with no office codes, office code and name are blank
    # In this case, funding office is present, awarding office is not
    obj = initialize_test_obj(awarding_office=None, funding_office=None, uri='7654321', record_type=1,
                              award_mod_amend='1')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_office_code'] is None
    assert obj['awarding_office_name'] is None
    assert obj['funding_office_code'] == '03aB03'
    assert obj['funding_office_name'] == 'Office'

    # if office_code is not present and valid uri is given but it's record type 2, everything should be empty
    obj = initialize_test_obj(awarding_office=None, funding_office=None, uri='654321', award_mod_amend='1')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_office_code'] is None
    assert obj['awarding_office_name'] is None
    assert obj['funding_office_code'] is None
    assert obj['funding_office_name'] is None

    # if office_code is not present and valid fain is given but it's record type 1, everything should be empty
    obj = initialize_test_obj(awarding_office=None, funding_office=None, fain='12345', record_type=1,
                              award_mod_amend='1')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_office_code'] is None
    assert obj['awarding_office_name'] is None
    assert obj['funding_office_code'] is None
    assert obj['funding_office_name'] is None

    # if office_code is not present and mod number is the same as the base record, do not derive it from historical data
    obj = initialize_test_obj(awarding_office=None, funding_office=None, fain='12345', award_mod_amend='0')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_office_code'] is None
    assert obj['awarding_office_name'] is None
    assert obj['funding_office_code'] is None
    assert obj['funding_office_name'] is None

    # if office_code is not present and mod number is the same as the base record, do not derive it from historical data
    obj = initialize_test_obj(awarding_office=None, funding_office=None, uri='654321', record_type=1,
                              award_mod_amend=None)
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_office_code'] is None
    assert obj['awarding_office_name'] is None
    assert obj['funding_office_code'] is None
    assert obj['funding_office_name'] is None

    # if office_code is not present and mod number is different from the base record, but the base office code is
    # invalid, do not derive
    obj = initialize_test_obj(awarding_office=None, funding_office=None, uri='efg', record_type=1,
                              award_mod_amend='1')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_office_code'] is None
    assert obj['awarding_office_name'] is None
    assert obj['funding_office_code'] is None
    assert obj['funding_office_name'] is None

    # if office_code is not present and mod number is different from the base record and the base office code is
    # valid but is not a grant/funding code, do not derive
    obj = initialize_test_obj(awarding_office=None, funding_office=None, uri='abcd', record_type=1,
                              award_mod_amend='1')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['awarding_office_code'] is None
    assert obj['awarding_office_name'] is None
    assert obj['funding_office_code'] is None
    assert obj['funding_office_name'] is None


def test_legal_country(database):
    initialize_db_values(database)

    # if primary_place_of_performance_country_code is present get country name
    obj = initialize_test_obj(legal_country='USA')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['legal_entity_country_name'] == 'United States of America'

    obj = initialize_test_obj(legal_country='NK')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['legal_entity_country_name'] is None


def test_derive_ppop_code(database):
    initialize_db_values(database)

    # Making sure nothing is changing if record type isn't 3
    obj = initialize_test_obj(record_type=1, legal_country='USA', le_zip5='12345')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_code'] == 'NY00000'

    # 00FORGN if country isn't USA
    obj = initialize_test_obj(record_type=3, ppop_code=None, legal_country='GBD')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_code'] == '00FORGN'

    # No derivation if country is USA and there is no state code
    obj = initialize_test_obj(record_type=3, ppop_code=None, legal_country='USA')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_code'] is None

    # Default to 00000 if legal entity city code is nothing and country is USA
    obj = initialize_test_obj(record_type=3, ppop_code=None, legal_country='USA', le_zip5='54321')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_code'] == 'NY00000'

    # Properly set city code if legal entity city code is there and country is USA
    obj = initialize_test_obj(record_type=3, ppop_code=None, legal_country='USA', le_zip5='12345')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_code'] == 'NY00001'


def test_derive_pii_redacted_ppop_data(database):
    initialize_db_values(database)

    # Test derivations when country code is USA
    obj = initialize_test_obj(record_type=3, legal_country='USA', le_zip5='54321')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_perform_country_c'] == 'USA'
    assert obj['place_of_perform_country_n'] == 'United States of America'
    assert obj['place_of_performance_city'] == 'Test City 2'
    assert obj['place_of_perform_county_co'] == '001'
    assert obj['place_of_perform_county_na'] == 'Test County'
    assert obj['place_of_perfor_state_code'] == 'NY'
    assert obj['place_of_perform_state_nam'] == 'New York'
    assert obj['place_of_performance_zip4a'] == '54321'
    assert obj['place_of_performance_congr'] == '05'

    # Test derivations when country code isn't USA
    obj = initialize_test_obj(record_type=3, legal_country='GBR', legal_foreign_city='London')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_perform_country_c'] == 'GBR'
    assert obj['place_of_perform_country_n'] == 'Great Britain'
    assert obj['place_of_performance_city'] == 'London'
    assert obj['place_of_performance_forei'] == 'London'


def test_split_zip(database):
    initialize_db_values(database)

    # testing with 5-digit
    obj = initialize_test_obj(ppop_zip4a='12345')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_zip5'] == '12345'
    assert obj['place_of_perform_zip_last4'] is None

    # testing with 9-digit
    obj = initialize_test_obj(ppop_zip4a='123456789')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_zip5'] == '12345'
    assert obj['place_of_perform_zip_last4'] == '6789'

    # testing with 9-digit and dash
    obj = initialize_test_obj(ppop_zip4a='12345-6789')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_zip5'] == '12345'
    assert obj['place_of_perform_zip_last4'] == '6789'

    # testing with city-wide
    obj = initialize_test_obj(ppop_zip4a='city-wide')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_zip5'] is None
    assert obj['place_of_perform_zip_last4'] is None

    # testing without ppop_zip4
    obj = initialize_test_obj(ppop_zip4a='')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_zip5'] is None
    assert obj['place_of_perform_zip_last4'] is None


def test_derive_parent_duns_single(database):
    initialize_db_values(database)

    obj = initialize_test_obj(awardee_or_recipient_uniqu='123456789')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)

    assert obj['ultimate_parent_legal_enti'] == 'Parent 1'
    assert obj['ultimate_parent_unique_ide'] == '234567890'


def test_derive_parent_duns_multiple(database):
    initialize_db_values(database)

    obj = initialize_test_obj(awardee_or_recipient_uniqu='234567890')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)

    assert obj['ultimate_parent_legal_enti'] == 'Parent 2'
    assert obj['ultimate_parent_unique_ide'] == '234567890'


def test_derive_parent_duns_no_parent_info(database):
    initialize_db_values(database)

    obj = initialize_test_obj(awardee_or_recipient_uniqu='345678901')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)

    assert obj['ultimate_parent_legal_enti'] is None
    assert obj['ultimate_parent_unique_ide'] is None


def test_derive_executive_compensation(database):
    initialize_db_values(database)

    # Test when values are null
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)

    # If the first 2 are null, the rest will be too
    assert obj['high_comp_officer1_full_na'] is None
    assert obj['high_comp_officer1_amount'] is None

    # Test when DUNS doesn't have exec comp data associated
    obj = initialize_test_obj(awardee_or_recipient_uniqu='345678901')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)

    # If the first 2 are null, the rest will be too
    assert obj['high_comp_officer1_full_na'] is None
    assert obj['high_comp_officer1_amount'] is None

    # Test with DUNS that has exec comp data
    obj = initialize_test_obj(awardee_or_recipient_uniqu='123456789')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)

    assert obj['high_comp_officer1_full_na'] == 'Officer 1'
    assert obj['high_comp_officer1_amount'] == '15'
    assert obj['high_comp_officer2_full_na'] == 'Officer 2'
    assert obj['high_comp_officer2_amount'] == '77.12'
    assert obj['high_comp_officer3_full_na'] == 'This is the third Officer'
    assert obj['high_comp_officer3_amount'] is None
    assert obj['high_comp_officer4_full_na'] is None
    assert obj['high_comp_officer4_amount'] == '0'
    assert obj['high_comp_officer5_full_na'] is None
    assert obj['high_comp_officer5_amount'] is None


def test_derive_labels(database):
    initialize_db_values(database)

    # Testing when these values are blank
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['action_type_description'] is None
    assert obj['assistance_type_desc'] is None
    assert obj['correction_delete_ind_desc'] is None
    assert obj['business_types_desc'] is None
    assert obj['business_funds_ind_desc'] is None

    # Testing for valid values of each
    obj = initialize_test_obj(cdi='c', action_type='a', assist_type='02', busi_type='d', busi_fund='non')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['action_type_description'] == ACTION_TYPE_DICT['A']
    assert obj['assistance_type_desc'] == ASSISTANCE_TYPE_DICT['02']
    assert obj['correction_delete_ind_desc'] == CORRECTION_DELETE_IND_DICT['C']
    assert obj['record_type_description'] == RECORD_TYPE_DICT[2]
    assert obj['business_types_desc'] == BUSINESS_TYPE_DICT['D']
    assert obj['business_funds_ind_desc'] == BUSINESS_FUNDS_IND_DICT['NON']

    # Testing for invalid values of each
    obj = initialize_test_obj(cdi='f', action_type='z', assist_type='01', record_type=5, busi_type='Z', busi_fund='ab')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['action_type_description'] is None
    assert obj['assistance_type_desc'] is None
    assert obj['correction_delete_ind_desc'] is None
    assert obj['record_type_description'] is None
    assert obj['business_types_desc'] is None
    assert obj['business_funds_ind_desc'] is None

    # Test multiple business types (2 valid, one invalid)
    obj = initialize_test_obj(cdi='f', action_type='z', assist_type='01', record_type=5, busi_type='azb')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['business_types_desc'] == BUSINESS_TYPE_DICT['A'] + ';' + BUSINESS_TYPE_DICT['B']


def test_is_active(database):
    initialize_db_values(database)

    # Testing with none values
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['is_active'] is True

    # Testing with value other than D
    obj = initialize_test_obj(cdi='c')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['is_active'] is True

    # Testing with D
    obj = initialize_test_obj(cdi='D')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['is_active'] is False


def test_derive_ppop_scope(database):
    initialize_db_values(database)

    # when ppop_zip4a is 5 digits and no congressional district
    obj = initialize_test_obj(ppop_zip4a='123454321', ppop_code='NY00001')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] == 'Single ZIP Code'

    # when ppop_zip4a is 5 digits and has congressional district
    obj = initialize_test_obj(ppop_zip4a='12345-4321', ppop_cd='03', ppop_code='NY0000r')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] == 'Single ZIP Code'

    # when ppop_zip4a is 5 digits
    obj = initialize_test_obj(ppop_zip4a='12345')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] == 'Single ZIP Code'

    # when ppop_zip4 is 9 digits but last 4 are invalid (one cd available)
    obj = initialize_test_obj(ppop_zip4a='543210000', ppop_code=None)
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] is None

    # when ppop_zip4 is 9 digits (no cd available)
    obj = initialize_test_obj(ppop_zip4a='987654321', ppop_code='NY0001r')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] == 'Single ZIP Code'

    # when ppop_zip4 is 'city-wide'
    obj = initialize_test_obj(ppop_zip4a='City-WIDE', ppop_code='NY0001R')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] == 'City-wide'

    # when we don't have ppop_zip4a and ppop_code is in XX**### format
    obj = initialize_test_obj(ppop_code='Ny**001')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] == 'County-wide'

    # when we don't have ppop_zip4a and ppop_code is in XX##### format
    obj = initialize_test_obj(ppop_code='Ny00001')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] == 'City-wide'

    # when we don't have ppop_zip4a and ppop_code is in 00##### format
    obj = initialize_test_obj(ppop_code='NY*****')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] == 'State-wide'

    # when we don't have ppop_zip4a and ppop_code is in 00##### daformat
    obj = initialize_test_obj(ppop_code='00*****')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] == 'Multi-state'

    # when we don't have ppop_zip4a and ppop_code is 00FORGN format
    obj = initialize_test_obj(ppop_code='00forgn')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] == 'Foreign'

    # when we don't have a ppop_code at all
    obj = initialize_test_obj(ppop_code='')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] is None

    # cases when ppop code is not provided but derived earlier
    obj = initialize_test_obj(record_type=3, ppop_code=None, legal_country='USA', le_zip5='54321')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] == 'Single ZIP Code'

    obj = initialize_test_obj(record_type=3, ppop_code=None, legal_country='GBD')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] == 'Foreign'

    obj = initialize_test_obj(record_type=1, legal_country='USA', le_zip5='12345')
    obj = fabs_derivations(obj, database.session, STATE_DICT, COUNTRY_DICT, SUB_TIER_DICT, CFDA_DICT, COUNTY_DICT,
                           OFFICE_DICT, EXEC_COMP_DICT)
    assert obj['place_of_performance_scope'] == 'City-wide'
