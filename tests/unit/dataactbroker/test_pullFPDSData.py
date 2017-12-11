import xmltodict
import os

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import SubTierAgency, CGAC, Zips

from dataactcore.scripts import pullFPDSData


def test_list_data():
    """ Test that list_data returns a list of data whether passed a list or a dict """
    assert isinstance(pullFPDSData.list_data([]), list)
    assert isinstance(pullFPDSData.list_data([1, 23, 3]), list)
    assert isinstance(pullFPDSData.list_data(["whatever", "will"]), list)
    assert isinstance(pullFPDSData.list_data({1: 4}), list)
    assert isinstance(pullFPDSData.list_data({'hello': 1}), list)


def test_extract_text():
    """ Test that extract_text returns the text passed if it's a string or gets #text from it if it's a dict """
    assert pullFPDSData.extract_text("test") == "test"
    assert pullFPDSData.extract_text({'#text': "test", '@description': "different content"}) == "test"


def test_get_county_by_zip(database):
    """ Test that getting the county by zip works """
    sess = database.session
    zip_code = Zips(zip5='12345', zip_last4='6789', county_number='000')
    sess.add(zip_code)
    sess.commit()

    assert pullFPDSData.get_county_by_zip(sess, 'abcde') is None
    assert pullFPDSData.get_county_by_zip(sess, '123456789') == '000'
    assert pullFPDSData.get_county_by_zip(sess, '12345') == '000'
    assert pullFPDSData.get_county_by_zip(sess, '123459876') == '000'
    assert pullFPDSData.get_county_by_zip(sess, '12345678') is None
    assert pullFPDSData.get_county_by_zip(sess, '56789') is None


def test_calculate_remaining_fields(database):
    """ Test that calculate_remaining_fields calculates fields based on content in the DB and inserts 999 for the code
        if the sub tier agency doesn't exist """
    sess = database.session
    cgac = CGAC(cgac_id=1, cgac_code='1700', agency_name='test name')
    zip_code = Zips(zip5='12345', zip_last4='6789', county_number='123')
    sub_tier = SubTierAgency(sub_tier_agency_code='0000', cgac_id=1)
    sess.add(cgac)
    sess.add(zip_code)
    sess.add(sub_tier)
    sess.commit()

    county_by_name = {'GA': {'COUNTY ONE': '123', 'GA COUNTY TWO': '321'},
                      'MD': {'JUST ONE MD': '024'}}
    county_by_code = {'MD': {'024': 'JUST ONE MD'},
                      'GA': {'123': 'COUNTY ONE'}}

    tmp_obj = pullFPDSData.calculate_remaining_fields({'awarding_sub_tier_agency_c': "0000",
                                                       'funding_sub_tier_agency_co': None,
                                                       'place_of_perform_county_na': 'JUST ONE MD',
                                                       'place_of_performance_state': 'MD',
                                                       'place_of_perform_country_c': 'USA',
                                                       'place_of_performance_zip4a': None,
                                                       'legal_entity_zip4': '987654321',
                                                       'legal_entity_country_code': 'USA',
                                                       'legal_entity_state_code': 'GA'},
                                                      sess,
                                                      {sub_tier.sub_tier_agency_code: sub_tier},
                                                      county_by_name,
                                                      county_by_code)
    tmp_obj_2 = pullFPDSData.calculate_remaining_fields({'awarding_sub_tier_agency_c': None,
                                                         'funding_sub_tier_agency_co': "0001",
                                                         'funding_sub_tier_agency_na': "Not Real",
                                                         'place_of_perform_county_na': 'JUST ONE MD',
                                                         'place_of_performance_state': 'GA',
                                                         'place_of_perform_country_c': 'USA',
                                                         'place_of_performance_zip4a': None,
                                                         'legal_entity_zip4': '123456789',
                                                         'legal_entity_country_code': 'USA',
                                                         'legal_entity_state_code': 'GA'},
                                                        sess,
                                                        {sub_tier.sub_tier_agency_code: sub_tier},
                                                        county_by_name,
                                                        county_by_code)
    tmp_obj_3 = pullFPDSData.calculate_remaining_fields({'awarding_sub_tier_agency_c': None,
                                                         'funding_sub_tier_agency_co': None,
                                                         'funding_sub_tier_agency_na': None,
                                                         'place_of_perform_county_na': None,
                                                         'place_of_performance_state': None,
                                                         'place_of_perform_country_c': 'USA',
                                                         'place_of_performance_zip4a': '123456789',
                                                         'legal_entity_zip4': '12345',
                                                         'legal_entity_country_code': 'USA',
                                                         'legal_entity_state_code': 'GA'},
                                                        sess,
                                                        {sub_tier.sub_tier_agency_code: sub_tier},
                                                        county_by_name,
                                                        county_by_code)
    assert tmp_obj['awarding_agency_code'] == '1700'
    assert tmp_obj['awarding_agency_name'] == 'test name'
    assert tmp_obj['place_of_perform_county_co'] == '024'
    assert tmp_obj['legal_entity_county_code'] is None
    assert tmp_obj['legal_entity_county_name'] is None
    assert tmp_obj_2['funding_agency_code'] == '999'
    assert tmp_obj_2['funding_agency_name'] is None
    assert tmp_obj_2['place_of_perform_county_co'] is None
    assert tmp_obj_2['legal_entity_county_code'] == '123'
    assert tmp_obj_2['legal_entity_county_name'] == 'COUNTY ONE'
    assert tmp_obj_3['place_of_perform_county_co'] == '123'
    assert tmp_obj_3['legal_entity_county_code'] == '123'
    assert tmp_obj_3['legal_entity_county_name'] == 'COUNTY ONE'


def test_process_data(database):
    sess = database.session
    fpds_file = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'fpdsXML.txt')
    f = open(fpds_file, "r")
    resp = f.read()
    f.close()

    resp_data = xmltodict.parse(resp, process_namespaces=True, namespaces={'http://www.fpdsng.com/FPDS': None,
                                                                           'http://www.w3.org/2005/Atom': None})

    listed_data = pullFPDSData.list_data(resp_data['feed']['entry'])

    tmp_obj_award = pullFPDSData.process_data(listed_data[0]['content']['award'], sess=sess, atom_type='award',
                                              sub_tier_list={}, county_by_name={}, county_by_code={})
    tmp_obj_idv = pullFPDSData.process_data(listed_data[1]['content']['IDV'], sess=sess, atom_type='IDV',
                                            sub_tier_list={}, county_by_name={}, county_by_code={})

    assert tmp_obj_award['piid'] == '0001'
    assert tmp_obj_award['major_program'] is None
    assert tmp_obj_award['place_of_performance_state'] == 'MD'
    assert tmp_obj_award['place_of_perfor_state_desc'] == 'MARYLAND'

    assert tmp_obj_idv['piid'] == '000000000LC3162'
    assert tmp_obj_idv['idv_type'] == 'B'
    assert tmp_obj_idv['idv_type_description'] == 'IDC'
    assert tmp_obj_idv['referenced_idv_type'] is None
