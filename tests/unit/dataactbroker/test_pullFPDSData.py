import xmltodict
import os

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import SubTierAgency, CGAC

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


def test_calculate_remaining_fields(database):
    """ Test that calculate_remaining_fields calculates fields based on content in the DB and inserts 999 for the code
        if the sub tier agency doesn't exist """
    cgac = CGAC(cgac_id=1, cgac_code='1700', agency_name='test name')
    sub_tier = SubTierAgency(sub_tier_agency_code='0000', cgac_id=1)
    database.session.add(cgac)
    database.session.add(sub_tier)
    database.session.commit()

    tmp_obj = pullFPDSData.calculate_remaining_fields({'awarding_sub_tier_agency_c': "0000",
                                                       'funding_sub_tier_agency_co': None},
                                                      {sub_tier.sub_tier_agency_code: sub_tier})
    tmp_obj_2 = pullFPDSData.calculate_remaining_fields({'awarding_sub_tier_agency_c': None,
                                                         'funding_sub_tier_agency_co': "0001",
                                                         'funding_sub_tier_agency_na': "Not Real"},
                                                        {sub_tier.sub_tier_agency_code: sub_tier})
    assert tmp_obj['awarding_agency_code'] == '1700'
    assert tmp_obj['awarding_agency_name'] == 'test name'
    assert tmp_obj_2['funding_agency_code'] == '999'
    assert tmp_obj_2['funding_agency_name'] is None


def test_process_data(database):
    fpds_file = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'fpdsXML.txt')
    f = open(fpds_file, "r")
    resp = f.read()
    f.close()

    resp_data = xmltodict.parse(resp, process_namespaces=True, namespaces={'http://www.fpdsng.com/FPDS': None,
                                                                           'http://www.w3.org/2005/Atom': None})

    listed_data = pullFPDSData.list_data(resp_data['feed']['entry'])

    tmp_obj_award = pullFPDSData.process_data(listed_data[0]['content']['award'], atom_type='award',
                                              sub_tier_list={})
    tmp_obj_idv = pullFPDSData.process_data(listed_data[1]['content']['IDV'], atom_type='IDV', sub_tier_list={})

    assert tmp_obj_award['piid'] == '0001'
    assert tmp_obj_award['major_program'] is None
    assert tmp_obj_award['place_of_performance_state'] == 'MD'
    assert tmp_obj_award['place_of_perfor_state_desc'] == 'MARYLAND'

    assert tmp_obj_idv['piid'] == '000000000LC3162'
    assert tmp_obj_idv['idv_type'] == 'B'
    assert tmp_obj_idv['idv_type_description'] == 'IDC'
    assert tmp_obj_idv['referenced_idv_type'] is None
