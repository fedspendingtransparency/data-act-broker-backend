from dataactbroker.helpers.fabs_derivations_helper import fabs_derivations
from dataactcore.models.lookups import (ACTION_TYPE_DICT, ASSISTANCE_TYPE_DICT, CORRECTION_DELETE_IND_DICT,
                                        RECORD_TYPE_DICT, BUSINESS_TYPE_DICT, BUSINESS_FUNDS_IND_DICT)
from dataactcore.models.stagingModels import PublishedAwardFinancialAssistance

from tests.unit.dataactcore.factories.domain import (ZipCityFactory, ZipsFactory, ZipsGroupedFactory, CityCodeFactory,
                                                     DunsFactory, CFDAProgramFactory, CGACFactory, FRECFactory,
                                                     SubTierAgencyFactory, OfficeFactory, StatesFactory,
                                                     CountyCodeFactory, CountryCodeFactory)
from tests.unit.dataactcore.factories.staging import PublishedAwardFinancialAssistanceFactory


def initialize_db_values(db):
    """ Initialize the values in the DB that can be used throughout the tests """
    # Zips
    zip_code_1 = ZipsFactory(zip5='12345', zip_last4='6789', state_abbreviation='NY', county_number='001',
                             congressional_district_no='01')
    zip_code_2 = ZipsFactory(zip5='12345', zip_last4='4321', state_abbreviation='NY', county_number='001',
                             congressional_district_no='02')
    zip_code_3 = ZipsFactory(zip5='54321', zip_last4='4321', state_abbreviation='NY', county_number='001',
                             congressional_district_no='05')
    zip_code_4 = ZipsFactory(zip5='98765', zip_last4='4321', state_abbreviation='NY', county_number='001',
                             congressional_district_no=None)
    # Grouped zips (we are assuming a correct SQL query on creation of this table)
    zips_grouped_1 = ZipsGroupedFactory(zip5='12345', state_abbreviation='NY', county_number='001',
                                        congressional_district_no='90')
    zips_grouped_2 = ZipsGroupedFactory(zip5='54321', state_abbreviation='NY', county_number='001',
                                        congressional_district_no='05')
    zips_grouped_3 = ZipsGroupedFactory(zip5='98765', state_abbreviation='NY', county_number='001',
                                        congressional_district_no='90')
    # Cities
    zip_city = ZipCityFactory(zip_code=zip_code_1.zip5, city_name='Test City')
    zip_city_2 = ZipCityFactory(zip_code=zip_code_3.zip5, city_name='Test City 2')
    zip_city_3 = ZipCityFactory(zip_code=zip_code_4.zip5, city_name='Test City 3')
    city_code = CityCodeFactory(feature_name='Test City', city_code='00001', state_code='NY',
                                county_number=zip_code_1.county_number, county_name='Test City County')
    # States
    state = StatesFactory(state_code='NY', state_name='New York')
    # Counties
    county = CountyCodeFactory(county_number='001', county_name='Test County', state_code='NY')
    # Countries
    country_1 = CountryCodeFactory(country_code='USA', country_name='United States of America')
    country_2 = CountryCodeFactory(country_code='GBR', country_name='Great Britain')
    # DUNS
    duns_1 = DunsFactory(awardee_or_recipient_uniqu='123456789', ultimate_parent_unique_ide='234567890',
                         ultimate_parent_legal_enti='Parent 1', high_comp_officer1_full_na='Officer 1',
                         high_comp_officer1_amount='15', high_comp_officer2_full_na='Officer 2',
                         high_comp_officer2_amount='77.12', high_comp_officer3_full_na='This is the third Officer',
                         high_comp_officer3_amount=None, high_comp_officer4_full_na=None,
                         high_comp_officer4_amount='0', high_comp_officer5_full_na=None,
                         high_comp_officer5_amount=None)
    duns_2a = DunsFactory(awardee_or_recipient_uniqu='234567890', ultimate_parent_unique_ide='234567890',
                          ultimate_parent_legal_enti='Parent 2', high_comp_officer1_full_na=None,
                          high_comp_officer1_amount=None, high_comp_officer2_full_na=None,
                          high_comp_officer2_amount=None, high_comp_officer3_full_na=None,
                          high_comp_officer3_amount=None, high_comp_officer4_full_na=None,
                          high_comp_officer4_amount=None, high_comp_officer5_full_na=None,
                          high_comp_officer5_amount=None)
    duns_2b = DunsFactory(awardee_or_recipient_uniqu='234567890', ultimate_parent_unique_ide=None,
                          ultimate_parent_legal_enti=None, high_comp_officer1_full_na=None,
                          high_comp_officer1_amount=None, high_comp_officer2_full_na=None,
                          high_comp_officer2_amount=None, high_comp_officer3_full_na=None,
                          high_comp_officer3_amount=None, high_comp_officer4_full_na=None,
                          high_comp_officer4_amount=None, high_comp_officer5_full_na=None,
                          high_comp_officer5_amount=None)
    duns_3 = DunsFactory(awardee_or_recipient_uniqu='345678901', ultimate_parent_unique_ide=None,
                         ultimate_parent_legal_enti=None, high_comp_officer1_full_na=None,
                         high_comp_officer1_amount=None, high_comp_officer2_full_na=None,
                         high_comp_officer2_amount=None, high_comp_officer3_full_na=None,
                         high_comp_officer3_amount=None, high_comp_officer4_full_na=None,
                         high_comp_officer4_amount=None, high_comp_officer5_full_na=None,
                         high_comp_officer5_amount=None)
    # CFDA
    cfda = CFDAProgramFactory(program_number=12.345, program_title='CFDA Title')
    # Agencies
    cgac_1 = CGACFactory(cgac_code='000', agency_name='Test CGAC Agency')
    cgac_2 = CGACFactory(cgac_code='111', agency_name='Test CGAC Agency 2')
    frec_1 = FRECFactory(frec_code='0000', agency_name='Test FREC Agency', cgac=cgac_1)
    frec_2 = FRECFactory(frec_code='1111', agency_name='Test FREC Agency 2', cgac=cgac_2)
    cgac_sub_tier = SubTierAgencyFactory(sub_tier_agency_code='12AB', sub_tier_agency_name='Test Subtier Agency',
                                         cgac=cgac_1, frec=frec_1, is_frec=False)
    frec_sub_tier = SubTierAgencyFactory(sub_tier_agency_code='4321', sub_tier_agency_name='Test FREC Subtier Agency',
                                         cgac=cgac_2, frec=frec_2, is_frec=True)
    valid_office = OfficeFactory(office_code='03AB03', office_name='Office', sub_tier_code='12Ab', agency_code='000',
                                 financial_assistance_awards_office=True, contract_funding_office=True,
                                 financial_assistance_funding_office=False)
    invalid_office = OfficeFactory(office_code='654321', office_name='Office', sub_tier_code='12Ab', agency_code='000',
                                   financial_assistance_awards_office=False, contract_funding_office=False,
                                   financial_assistance_funding_office=False)
    # record type 2 pafas
    pafa_1 = PublishedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='12aB', fain='12345', uri='123456',
                                                      action_date='04/28/2000', funding_office_code=None,
                                                      awarding_office_code='03aB03', is_active=True, record_type=2,
                                                      award_modification_amendme='0', submission_id=1)
    pafa_2 = PublishedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='12aB', fain='123456', uri='1234567',
                                                      action_date='04/28/2000', funding_office_code='03aB03',
                                                      awarding_office_code=None, is_active=True, record_type=2,
                                                      award_modification_amendme=None, submission_id=1)
    # record type 1 pafas
    pafa_3 = PublishedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='12aB', fain='54321', uri='654321',
                                                      action_date='04/28/2000', funding_office_code=None,
                                                      awarding_office_code='03aB03', is_active=True, record_type=1,
                                                      award_modification_amendme=None, submission_id=1)
    pafa_4 = PublishedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='12aB', fain='654321', uri='7654321',
                                                      action_date='04/28/2000', funding_office_code='03aB03',
                                                      awarding_office_code=None, is_active=True, record_type=1,
                                                      award_modification_amendme='0', submission_id=1)
    # record type 1 base pafa with invalid office codes
    pafa_5 = PublishedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='12aB', fain='abcd', uri='efg',
                                                      action_date='04/28/2000', funding_office_code='123456',
                                                      awarding_office_code='123456', is_active=True, record_type=1,
                                                      award_modification_amendme='0', submission_id=1)
    # record type 1 base pafa with valid office codes but they aren't grant or funding type
    pafa_6 = PublishedAwardFinancialAssistanceFactory(awarding_sub_tier_agency_c='12aB', fain='efg', uri='abcd',
                                                      action_date='04/28/2000', funding_office_code='654321',
                                                      awarding_office_code='654321', is_active=True, record_type=1,
                                                      award_modification_amendme='0', submission_id=1)
    db.session.add_all([zip_code_1, zip_code_2, zip_code_3, zip_code_4, zips_grouped_1, zips_grouped_2, zips_grouped_3,
                        zip_city, zip_city_2, zip_city_3, city_code, state, county, country_1, country_2, duns_1,
                        duns_2a, duns_2b, duns_3, cfda, cgac_1, cgac_2, frec_1, frec_2, cgac_sub_tier, frec_sub_tier,
                        valid_office, invalid_office, pafa_1, pafa_2, pafa_3, pafa_4, pafa_5, pafa_6])
    db.session.commit()


def stringify(value):
    """ Stringify a value to insert cleanly into the test DB. """
    return '\'{}\''.format(value) if value else 'NULL'


def initialize_test_row(db, fao=None, nffa=None, cfda_num='00.000', sub_tier_code='12aB', sub_fund_agency_code=None,
                        ppop_code='NY00000', ppop_zip4a=None, ppop_cd=None, le_zip5=None, le_zip4=None, record_type=2,
                        award_mod_amend=None, fain=None, uri=None, cdi=None, awarding_office='03ab03',
                        funding_office='03ab03', legal_congr=None, primary_place_country='USA', legal_country='USA',
                        legal_foreign_city=None, action_type=None, assist_type=None, busi_type=None, busi_fund=None,
                        awardee_or_recipient_uniqu=None, submission_id=9999):
    """ Initialize the values in the object being run through the fabs_derivations function """
    column_list = [col.key for col in PublishedAwardFinancialAssistance.__table__.columns]
    remove_cols = ['created_at', 'updated_at', 'modified_at', 'is_active',
                   'published_award_financial_assistance_id']
    for remove_col in remove_cols:
        column_list.remove(remove_col)
    col_string = ", ".join(column_list)

    create_query = """
        DROP TABLE IF EXISTS tmp_fabs_{submission_id};

        CREATE TABLE tmp_fabs_{submission_id}
        AS
            SELECT {cols}
            FROM published_award_financial_assistance
            WHERE false;

        TRUNCATE TABLE tmp_fabs_{submission_id};

        ALTER TABLE tmp_fabs_{submission_id} ADD COLUMN published_award_financial_assistance_id SERIAL PRIMARY KEY;
    """
    db.session.execute(create_query.format(submission_id=submission_id, cols=col_string))

    insert_query = """
        INSERT INTO tmp_fabs_{submission_id} (federal_action_obligation, non_federal_funding_amount, cfda_number,
            awarding_sub_tier_agency_c, funding_sub_tier_agency_co, place_of_performance_code,
            place_of_performance_zip4a, place_of_performance_congr, legal_entity_zip5, legal_entity_zip_last4,
            record_type, award_modification_amendme, fain, uri, correction_delete_indicatr, awarding_office_code,
            funding_office_code, legal_entity_congressional, place_of_perform_country_c, legal_entity_country_code,
            legal_entity_foreign_city, awardee_or_recipient_uniqu, action_type, assistance_type, business_types,
            business_funds_indicator)
        VALUES ({fao}, {nffa}, {cfda_num}, {sub_tier_code}, {sub_fund_agency_code}, {ppop_code}, {ppop_zip4a},
            {ppop_cd}, {le_zip5}, {le_zip4}, {record_type}, {award_mod_amend}, {fain}, {uri}, {cdi}, {awarding_office},
            {funding_office}, {legal_congr}, {primary_place_country}, {legal_country}, {legal_foreign_city},
            {awardee_or_recipient_uniqu}, {action_type}, {assist_type}, {busi_type}, {busi_fund})
    """.format(submission_id=submission_id,
               fao=fao if fao else 'NULL',
               nffa=nffa if nffa else 'NULL',
               cfda_num=stringify(cfda_num),
               sub_tier_code=stringify(sub_tier_code),
               sub_fund_agency_code=stringify(sub_fund_agency_code),
               ppop_code=stringify(ppop_code),
               ppop_zip4a=stringify(ppop_zip4a),
               ppop_cd=stringify(ppop_cd),
               le_zip5=stringify(le_zip5),
               le_zip4=stringify(le_zip4),
               record_type=record_type if record_type else 'NULL',
               award_mod_amend=stringify(award_mod_amend),
               fain=stringify(fain),
               uri=stringify(uri),
               cdi=stringify(cdi),
               awarding_office=stringify(awarding_office),
               funding_office=stringify(funding_office),
               legal_congr=stringify(legal_congr),
               primary_place_country=stringify(primary_place_country),
               legal_country=stringify(legal_country),
               legal_foreign_city=stringify(legal_foreign_city),
               awardee_or_recipient_uniqu=stringify(awardee_or_recipient_uniqu),
               action_type=stringify(action_type),
               assist_type=stringify(assist_type),
               busi_type=stringify(busi_type),
               busi_fund=stringify(busi_fund))
    db.session.execute(insert_query)
    db.session.commit()
    return submission_id


def get_derived_fabs(db, submission_id):
    """ Retrieve the derived submission information. """
    res = db.session.execute('SELECT * FROM tmp_fabs_{submission_id}'.format(submission_id=submission_id))
    # derived_fabs = db.session.query(PublishedAwardFinancialAssistance).filter_by(submission_id=submission_id).first()
    return res.fetchone()


def test_total_funding_amount(database):
    initialize_db_values(database)

    # when fao and nffa are empty
    submission_id = initialize_test_row(database, submission_id=2)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.total_funding_amount == '0'

    # when one of fao and nffa is empty and the other isn't
    submission_id = initialize_test_row(database, fao=5.3, submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.total_funding_amount == '5.3'

    # when fao and nffa aren't empty
    submission_id = initialize_test_row(database, fao=-10.6, nffa=123, submission_id=4)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.total_funding_amount == '112.4'


def test_cfda_title(database):
    initialize_db_values(database)

    # when cfda_number isn't in the database
    submission_id = initialize_test_row(database, submission_id=2)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.cfda_title is None

    # when cfda_number is in the database
    submission_id = initialize_test_row(database, cfda_num='12.345', submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.cfda_title == 'CFDA Title'


def test_awarding_agency_cgac(database):
    initialize_db_values(database)

    submission_id = initialize_test_row(database, sub_tier_code='12ab')
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_agency_code == '000'
    assert fabs_obj.awarding_agency_name == 'Test CGAC Agency'
    assert fabs_obj.awarding_sub_tier_agency_n == 'Test Subtier Agency'


def test_awarding_agency_frec(database):
    initialize_db_values(database)

    submission_id = initialize_test_row(database, sub_tier_code='4321')
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_agency_code == '1111'
    assert fabs_obj.awarding_agency_name == 'Test FREC Agency 2'
    assert fabs_obj.awarding_sub_tier_agency_n == 'Test FREC Subtier Agency'


def test_funding_sub_tier_agency_na(database):
    initialize_db_values(database)

    # when funding_sub_tier_agency_co is not provided, it should get derived
    submission_id = initialize_test_row(database, submission_id=2)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.funding_agency_code == '000'
    assert fabs_obj.funding_agency_name == 'Test CGAC Agency'
    assert fabs_obj.funding_sub_tier_agency_na == 'Test Subtier Agency'

    # when funding_sub_tier_agency_co is provided
    submission_id = initialize_test_row(database, sub_fund_agency_code='4321', submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.funding_agency_code == '1111'
    assert fabs_obj.funding_agency_name == 'Test FREC Agency 2'
    assert fabs_obj.funding_sub_tier_agency_na == 'Test FREC Subtier Agency'


def test_ppop_state(database):
    initialize_db_values(database)

    submission_id = initialize_test_row(database, submission_id=2)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_perfor_state_code == 'NY'
    assert fabs_obj.place_of_perform_state_nam == 'New York'

    submission_id = initialize_test_row(database, ppop_code='00*****', submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_perfor_state_code is None
    assert fabs_obj.place_of_perform_state_nam == 'Multi-state'

    submission_id = initialize_test_row(database, ppop_code='', record_type=3, submission_id=4)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_perfor_state_code is None
    assert fabs_obj.place_of_perform_state_nam is None


def test_ppop_derivations(database):
    initialize_db_values(database)

    # when ppop_zip4a is 5 digits and no congressional district
    submission_id = initialize_test_row(database, ppop_zip4a='123454321', ppop_code='NY00001', submission_id=2)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_congr == '02'
    assert fabs_obj.place_of_perform_county_co == '001'
    assert fabs_obj.place_of_perform_county_na == 'Test County'
    assert fabs_obj.place_of_performance_city == 'Test City'

    # when ppop_zip4a is 5 digits and has congressional district
    submission_id = initialize_test_row(database, ppop_zip4a='12345-4321', ppop_cd='03', ppop_code='NY0000r',
                                        submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_congr == '03'

    # when ppop_zip4a is 5 digits
    submission_id = initialize_test_row(database, ppop_zip4a='12345', submission_id=4)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    # cd should be 90 if there's more than one option
    assert fabs_obj.place_of_performance_congr == '90'
    assert fabs_obj.place_of_perform_county_co == '001'
    assert fabs_obj.place_of_perform_county_na == 'Test County'
    assert fabs_obj.place_of_performance_city == 'Test City'

    # when ppop_zip4 is 9 digits but last 4 are invalid (one cd available)
    submission_id = initialize_test_row(database, ppop_zip4a='543210000', ppop_code='NY00000', submission_id=5)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_congr == '05'
    assert fabs_obj.place_of_perform_county_co == '001'
    assert fabs_obj.place_of_perform_county_na == 'Test County'
    assert fabs_obj.place_of_performance_city == 'Test City 2'

    # when ppop_zip4 is 9 digits (no cd available)
    submission_id = initialize_test_row(database, ppop_zip4a='987654321', ppop_code='NY0001r', submission_id=6)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_congr == '90'
    assert fabs_obj.place_of_perform_county_co == '001'
    assert fabs_obj.place_of_perform_county_na == 'Test County'
    assert fabs_obj.place_of_performance_city == 'Test City 3'

    # when ppop_zip4 is 'city-wide'
    submission_id = initialize_test_row(database, ppop_zip4a='City-WIDE', ppop_code='NY0001R', submission_id=7)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_congr is None
    assert fabs_obj.place_of_perform_county_co is None
    assert fabs_obj.place_of_perform_county_na is None
    assert fabs_obj.place_of_performance_city is None

    # when we don't have ppop_zip4a and ppop_code is in XX**### format
    submission_id = initialize_test_row(database, ppop_code='Ny**001', submission_id=8)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_perform_county_co == '001'
    assert fabs_obj.place_of_perform_county_na == 'Test County'
    assert fabs_obj.place_of_performance_city is None
    assert fabs_obj.place_of_performance_congr is None

    # when we don't have ppop_zip4a and ppop_code is in XX##### format
    submission_id = initialize_test_row(database, ppop_code='Ny00001', submission_id=9)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_perform_county_co == '001'
    assert fabs_obj.place_of_perform_county_na == 'Test City County'
    assert fabs_obj.place_of_performance_city == 'Test City'
    assert fabs_obj.place_of_performance_congr is None

    # when we don't have a ppop_code at all
    submission_id = initialize_test_row(database, ppop_code='', record_type=3, submission_id=10)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_perform_county_co is None
    assert fabs_obj.place_of_perform_county_na is None
    assert fabs_obj.place_of_performance_city is None
    assert fabs_obj.place_of_performance_congr is None


def test_legal_entity_derivations(database):
    initialize_db_values(database)

    # if there is a legal_entity_zip5, record_type is always 2 or 3
    # when we have legal_entity_zip5 and zip4
    submission_id = initialize_test_row(database, le_zip5='12345', le_zip4='6789', submission_id=2)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.legal_entity_city_name == 'Test City'
    assert fabs_obj.legal_entity_congressional == '01'
    assert fabs_obj.legal_entity_county_code == '001'
    assert fabs_obj.legal_entity_county_name == 'Test County'
    assert fabs_obj.legal_entity_state_code == 'NY'
    assert fabs_obj.legal_entity_state_name == 'New York'

    # when we have legal_entity_zip5 but no zip4
    submission_id = initialize_test_row(database, le_zip5='12345', submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.legal_entity_city_name == 'Test City'
    # there are multiple options so this should be 90
    assert fabs_obj.legal_entity_congressional == '90'
    assert fabs_obj.legal_entity_county_code == '001'
    assert fabs_obj.legal_entity_county_name == 'Test County'
    assert fabs_obj.legal_entity_state_code == 'NY'
    assert fabs_obj.legal_entity_state_name == 'New York'

    # when we have legal_entity_zip5 and congressional but no zip4
    submission_id = initialize_test_row(database, le_zip5='12345', legal_congr='95', submission_id=4)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.legal_entity_congressional == '95'

    # if there is no legal_entity_zip5 and record_type is 1, ppop_code is always in format XX**###
    submission_id = initialize_test_row(database, record_type=1, ppop_cd='03', ppop_code='NY**001', submission_id=5)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.legal_entity_city_name is None
    assert fabs_obj.legal_entity_congressional == '03'
    assert fabs_obj.legal_entity_county_code == '001'
    assert fabs_obj.legal_entity_county_name == 'Test County'
    assert fabs_obj.legal_entity_state_code == 'NY'
    assert fabs_obj.legal_entity_state_name == 'New York'

    # if there is no legal_entity_zip5, record_type is always 1 and ppop_code can be format XX*****
    submission_id = initialize_test_row(database, record_type=1, ppop_cd='99', ppop_code='NY*****', submission_id=6)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.legal_entity_city_name is None
    assert fabs_obj.legal_entity_congressional == '99'
    assert fabs_obj.legal_entity_county_code is None
    assert fabs_obj.legal_entity_county_name is None
    assert fabs_obj.legal_entity_state_code == 'NY'
    assert fabs_obj.legal_entity_state_name == 'New York'

    # if there is no legal_entity_zip5, record_type is always 1 and ppop_code can be format 00FORGN
    submission_id = initialize_test_row(database, record_type=1, ppop_cd=None, ppop_code='00FORGN', submission_id=7)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.legal_entity_city_name is None
    assert fabs_obj.legal_entity_congressional is None
    assert fabs_obj.legal_entity_county_code is None
    assert fabs_obj.legal_entity_county_name is None
    assert fabs_obj.legal_entity_state_code is None
    assert fabs_obj.legal_entity_state_name is None


def test_primary_place_country(database):
    initialize_db_values(database)

    # if primary_plce_of_performance_country_code is present get country name
    submission_id = initialize_test_row(database, primary_place_country='USA', submission_id=2)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_perform_country_n == 'United States of America'

    submission_id = initialize_test_row(database, primary_place_country='NK', submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_perform_country_n is None


def test_derive_office_data(database):
    initialize_db_values(database)

    # if office_code is present, get office name
    submission_id = initialize_test_row(database, submission_id=2)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_office_name == 'Office'
    assert fabs_obj.funding_office_name == 'Office'

    # if office_code is not present, derive it from historical data (record type 2 or 3 uses fain, ignores uri)
    # In this case, there is no funding office but there is an awarding office
    submission_id = initialize_test_row(database, awarding_office=None, funding_office=None, fain='12345',
                                        award_mod_amend='1', submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_office_code == '03AB03'
    assert fabs_obj.awarding_office_name == 'Office'
    assert fabs_obj.funding_office_code is None
    assert fabs_obj.funding_office_name is None

    # if office_code is not present, and no valid fain is given, office code and name are blank
    submission_id = initialize_test_row(database, awarding_office=None, funding_office=None, fain='54321',
                                        award_mod_amend='1', submission_id=4)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_office_code is None
    assert fabs_obj.awarding_office_name is None
    assert fabs_obj.funding_office_code is None
    assert fabs_obj.funding_office_name is None

    # if office_code is not present, and valid fain is given, with no office codes, office code and name are blank
    # In this case, funding office is present, awarding office is not
    submission_id = initialize_test_row(database, awarding_office=None, funding_office=None, fain='123456',
                                        award_mod_amend='1', submission_id=5)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_office_code is None
    assert fabs_obj.awarding_office_name is None
    assert fabs_obj.funding_office_code == '03AB03'
    assert fabs_obj.funding_office_name == 'Office'

    # if office_code is not present, derive it from historical data (record type 1 uses uri, ignores fain)
    # In this case, awarding office is present, funding office is not
    submission_id = initialize_test_row(database, awarding_office=None, funding_office=None, uri='654321',
                                        record_type=1, award_mod_amend='1', submission_id=6)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_office_code == '03AB03'
    assert fabs_obj.awarding_office_name == 'Office'
    assert fabs_obj.funding_office_code is None
    assert fabs_obj.funding_office_name is None

    # if office_code is not present, and no valid uri is given, office code and name are blank
    submission_id = initialize_test_row(database, awarding_office=None, funding_office=None, uri='54321', record_type=1,
                                        award_mod_amend='1', submission_id=7)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_office_code is None
    assert fabs_obj.awarding_office_name is None
    assert fabs_obj.funding_office_code is None
    assert fabs_obj.funding_office_name is None

    # if office_code is not present, and valid uri is given, with no office codes, office code and name are blank
    # In this case, funding office is present, awarding office is not
    submission_id = initialize_test_row(database, awarding_office=None, funding_office=None, uri='7654321',
                                        record_type=1, award_mod_amend='1', submission_id=8)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_office_code is None
    assert fabs_obj.awarding_office_name is None
    assert fabs_obj.funding_office_code == '03AB03'
    assert fabs_obj.funding_office_name == 'Office'

    # if office_code is not present and valid uri is given but it's record type 2, everything should be empty
    submission_id = initialize_test_row(database, awarding_office=None, funding_office=None, uri='654321',
                                        award_mod_amend='1', submission_id=9)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_office_code is None
    assert fabs_obj.awarding_office_name is None
    assert fabs_obj.funding_office_code is None
    assert fabs_obj.funding_office_name is None

    # if office_code is not present and valid fain is given but it's record type 1, everything should be empty
    submission_id = initialize_test_row(database, awarding_office=None, funding_office=None, fain='12345',
                                        record_type=1, award_mod_amend='1', submission_id=10)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_office_code is None
    assert fabs_obj.awarding_office_name is None
    assert fabs_obj.funding_office_code is None
    assert fabs_obj.funding_office_name is None

    # if office_code is not present and mod number is the same as the base record, do not derive it from historical data
    submission_id = initialize_test_row(database, awarding_office=None, funding_office=None, fain='12345',
                                        award_mod_amend='0', submission_id=11)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_office_code is None
    assert fabs_obj.awarding_office_name is None
    assert fabs_obj.funding_office_code is None
    assert fabs_obj.funding_office_name is None

    # if office_code is not present and mod number is the same as the base record, do not derive it from historical data
    submission_id = initialize_test_row(database, awarding_office=None, funding_office=None, uri='654321',
                                        record_type=1, award_mod_amend=None, submission_id=12)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_office_code is None
    assert fabs_obj.awarding_office_name is None
    assert fabs_obj.funding_office_code is None
    assert fabs_obj.funding_office_name is None

    # if office_code is not present and mod number is different from the base record, but the base office code is
    # invalid, do not derive
    submission_id = initialize_test_row(database, awarding_office=None, funding_office=None, uri='efg', record_type=1,
                                        award_mod_amend='1', submission_id=13)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_office_code is None
    assert fabs_obj.awarding_office_name is None
    assert fabs_obj.funding_office_code is None
    assert fabs_obj.funding_office_name is None

    # if office_code is not present and mod number is different from the base record and the base office code is
    # valid but is not a grant/funding code, do not derive
    submission_id = initialize_test_row(database, awarding_office=None, funding_office=None, uri='abcd', record_type=1,
                                        award_mod_amend='1', submission_id=14)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.awarding_office_code is None
    assert fabs_obj.awarding_office_name is None
    assert fabs_obj.funding_office_code is None
    assert fabs_obj.funding_office_name is None


def test_legal_country(database):
    initialize_db_values(database)

    # if primary_place_of_performance_country_code is present get country name
    submission_id = initialize_test_row(database, legal_country='USA', submission_id=2)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.legal_entity_country_name == 'United States of America'

    submission_id = initialize_test_row(database, legal_country='NK', submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.legal_entity_country_name is None


def test_derive_ppop_code(database):
    initialize_db_values(database)

    # Making sure nothing is changing if record type isn't 3
    submission_id = initialize_test_row(database, record_type=1, legal_country='USA', le_zip5='12345', submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_code == 'NY00000'

    # 00FORGN if country isn't USA
    submission_id = initialize_test_row(database, record_type=3, ppop_code=None, legal_country='GBD', submission_id=4)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_code == '00FORGN'

    # No derivation if country is USA and there is no state code
    submission_id = initialize_test_row(database, record_type=3, ppop_code=None, legal_country='USA', submission_id=5)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_code is None

    # Default to 00000 if legal entity city code is nothing and country is USA
    submission_id = initialize_test_row(database, record_type=3, ppop_code=None, legal_country='USA', le_zip5='54321',
                                        submission_id=6)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_code == 'NY00000'

    # Properly set city code if legal entity city code is there and country is USA
    submission_id = initialize_test_row(database, record_type=3, ppop_code=None, legal_country='USA', le_zip5='12345',
                                        submission_id=7)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_code == 'NY00001'


def test_derive_pii_redacted_ppop_data(database):
    initialize_db_values(database)

    # Test derivations when country code is USA
    submission_id = initialize_test_row(database, record_type=3, legal_country='USA', le_zip5='54321', submission_id=2)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_perform_country_c == 'USA'
    assert fabs_obj.place_of_perform_country_n == 'United States of America'
    assert fabs_obj.place_of_performance_city == 'Test City 2'
    assert fabs_obj.place_of_perform_county_co == '001'
    assert fabs_obj.place_of_perform_county_na == 'Test County'
    assert fabs_obj.place_of_perfor_state_code == 'NY'
    assert fabs_obj.place_of_perform_state_nam == 'New York'
    assert fabs_obj.place_of_performance_zip4a == '54321'
    assert fabs_obj.place_of_performance_congr == '05'

    # Test derivations when country code isn't USA
    submission_id = initialize_test_row(database, record_type=3, legal_country='GBR', legal_foreign_city='London',
                                        submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_perform_country_c == 'GBR'
    assert fabs_obj.place_of_perform_country_n == 'Great Britain'
    assert fabs_obj.place_of_performance_city == 'London'
    assert fabs_obj.place_of_performance_forei == 'London'


def test_split_zip(database):
    initialize_db_values(database)

    # testing with 5-digit
    submission_id = initialize_test_row(database, ppop_zip4a='12345', submission_id=2)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_zip5 == '12345'
    assert fabs_obj.place_of_perform_zip_last4 is None

    # testing with 9-digit
    submission_id = initialize_test_row(database, ppop_zip4a='123456789', submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_zip5 == '12345'
    assert fabs_obj.place_of_perform_zip_last4 == '6789'

    # testing with 9-digit and dash
    submission_id = initialize_test_row(database, ppop_zip4a='12345-6789', submission_id=4)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_zip5 == '12345'
    assert fabs_obj.place_of_perform_zip_last4 == '6789'

    # testing with city-wide
    submission_id = initialize_test_row(database, ppop_zip4a='city-wide', submission_id=5)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_zip5 is None
    assert fabs_obj.place_of_perform_zip_last4 is None

    # testing without ppop_zip4
    submission_id = initialize_test_row(database, ppop_zip4a='', submission_id=6)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_zip5 is None
    assert fabs_obj.place_of_perform_zip_last4 is None


def test_derive_parent_duns_single(database):
    initialize_db_values(database)

    submission_id = initialize_test_row(database, awardee_or_recipient_uniqu='123456789')
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.ultimate_parent_legal_enti == 'Parent 1'
    assert fabs_obj.ultimate_parent_unique_ide == '234567890'


def test_derive_parent_duns_multiple(database):
    initialize_db_values(database)

    submission_id = initialize_test_row(database, awardee_or_recipient_uniqu='234567890')
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.ultimate_parent_legal_enti == 'Parent 2'
    assert fabs_obj.ultimate_parent_unique_ide == '234567890'


def test_derive_parent_duns_no_parent_info(database):
    initialize_db_values(database)

    submission_id = initialize_test_row(database, awardee_or_recipient_uniqu='345678901')
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.ultimate_parent_legal_enti is None
    assert fabs_obj.ultimate_parent_unique_ide is None


def test_derive_executive_compensation(database):
    initialize_db_values(database)

    # Test when values are null
    submission_id = initialize_test_row(database, submission_id=2)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    # If the first 2 are null, the rest will be too
    assert fabs_obj.high_comp_officer1_full_na is None
    assert fabs_obj.high_comp_officer1_amount is None

    # Test when DUNS doesn't have exec comp data associated
    submission_id = initialize_test_row(database, awardee_or_recipient_uniqu='345678901', submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    # If the first 2 are null, the rest will be too
    assert fabs_obj.high_comp_officer1_full_na is None
    assert fabs_obj.high_comp_officer1_amount is None

    # Test with DUNS that has exec comp data
    submission_id = initialize_test_row(database, awardee_or_recipient_uniqu='123456789', submission_id=4)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.high_comp_officer1_full_na == 'Officer 1'
    assert fabs_obj.high_comp_officer1_amount == '15'
    assert fabs_obj.high_comp_officer2_full_na == 'Officer 2'
    assert fabs_obj.high_comp_officer2_amount == '77.12'
    assert fabs_obj.high_comp_officer3_full_na == 'This is the third Officer'
    assert fabs_obj.high_comp_officer3_amount is None
    assert fabs_obj.high_comp_officer4_full_na is None
    assert fabs_obj.high_comp_officer4_amount == '0'
    assert fabs_obj.high_comp_officer5_full_na is None
    assert fabs_obj.high_comp_officer5_amount is None


def test_derive_labels(database):
    initialize_db_values(database)

    # Testing when these values are blank
    submission_id = initialize_test_row(database, submission_id=2)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.action_type_description is None
    assert fabs_obj.assistance_type_desc is None
    assert fabs_obj.correction_delete_ind_desc is None
    assert fabs_obj.business_types_desc is None
    assert fabs_obj.business_funds_ind_desc is None

    # Testing for valid values of each
    submission_id = initialize_test_row(database, cdi='c', action_type='a', assist_type='02', busi_type='d',
                                        busi_fund='non', submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.action_type_description == ACTION_TYPE_DICT['A']
    assert fabs_obj.assistance_type_desc == ASSISTANCE_TYPE_DICT['02']
    assert fabs_obj.correction_delete_ind_desc == CORRECTION_DELETE_IND_DICT['C']
    assert fabs_obj.record_type_description == RECORD_TYPE_DICT[2]
    assert fabs_obj.business_types_desc == BUSINESS_TYPE_DICT['D']
    assert fabs_obj.business_funds_ind_desc == BUSINESS_FUNDS_IND_DICT['NON']

    # Testing for invalid values of each
    submission_id = initialize_test_row(database, cdi='f', action_type='z', assist_type='01', record_type=5,
                                        busi_type='Z', busi_fund='ab', submission_id=4)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.action_type_description is None
    assert fabs_obj.assistance_type_desc is None
    assert fabs_obj.correction_delete_ind_desc is None
    assert fabs_obj.record_type_description is None
    assert fabs_obj.business_types_desc is None
    assert fabs_obj.business_funds_ind_desc is None

    # Test multiple business types (2 valid, 1 invalid)
    submission_id = initialize_test_row(database, cdi='f', action_type='z', assist_type='01', record_type=5,
                                        busi_type='azb', submission_id=5)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.business_types_desc == BUSINESS_TYPE_DICT['A'] + ';' + BUSINESS_TYPE_DICT['B']


def test_derive_ppop_scope(database):
    initialize_db_values(database)

    # when ppop_zip4a is 5 digits and no congressional district
    submission_id = initialize_test_row(database, ppop_zip4a='123454321', ppop_code='NY00001', submission_id=2)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope == 'Single ZIP Code'

    # when ppop_zip4a is 5 digits and has congressional district
    submission_id = initialize_test_row(database, ppop_zip4a='12345-4321', ppop_cd='03', ppop_code='NY0000r',
                                        submission_id=3)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope == 'Single ZIP Code'

    # when ppop_zip4a is 5 digits
    submission_id = initialize_test_row(database, ppop_zip4a='12345', submission_id=4)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope == 'Single ZIP Code'

    # when ppop_zip4 is 9 digits but last 4 are invalid (one cd available)
    submission_id = initialize_test_row(database, ppop_zip4a='543210000', ppop_code=None, submission_id=5)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope is None

    # when ppop_zip4 is 9 digits (no cd available)
    submission_id = initialize_test_row(database, ppop_zip4a='987654321', ppop_code='NY0001r', submission_id=6)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope == 'Single ZIP Code'

    # when ppop_zip4 is 'city-wide'
    submission_id = initialize_test_row(database, ppop_zip4a='City-WIDE', ppop_code='NY0001R', submission_id=7)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope == 'City-wide'

    # when we don't have ppop_zip4a and ppop_code is in XX**### format
    submission_id = initialize_test_row(database, ppop_code='Ny**001', submission_id=8)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope == 'County-wide'

    # when we don't have ppop_zip4a and ppop_code is in XX##### format
    submission_id = initialize_test_row(database, ppop_code='Ny00001', submission_id=9)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope == 'City-wide'

    # when we don't have ppop_zip4a and ppop_code is in 00##### format
    submission_id = initialize_test_row(database, ppop_code='NY*****', submission_id=10)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope == 'State-wide'

    # when we don't have ppop_zip4a and ppop_code is in 00##### daformat
    submission_id = initialize_test_row(database, ppop_code='00*****', submission_id=11)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope == 'Multi-state'

    # when we don't have ppop_zip4a and ppop_code is 00FORGN format
    submission_id = initialize_test_row(database, ppop_code='00forgn', submission_id=12)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope == 'Foreign'

    # when we don't have a ppop_code at all
    submission_id = initialize_test_row(database, ppop_code='', submission_id=13)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope is None

    # cases when ppop code is not provided but derived earlier
    submission_id = initialize_test_row(database, record_type=3, ppop_code=None, legal_country='USA', le_zip5='54321',
                                        submission_id=14)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope == 'Single ZIP Code'

    submission_id = initialize_test_row(database, record_type=3, ppop_code=None, legal_country='GBD', submission_id=15)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope == 'Foreign'

    submission_id = initialize_test_row(database, record_type=1, legal_country='USA', le_zip5='12345', submission_id=16)
    fabs_derivations(database.session, submission_id)
    database.session.commit()
    fabs_obj = get_derived_fabs(database, submission_id)
    assert fabs_obj.place_of_performance_scope == 'City-wide'
