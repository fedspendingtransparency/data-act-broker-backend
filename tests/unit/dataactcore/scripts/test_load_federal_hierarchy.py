import os
import json
import re
import datetime
from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory, SubTierAgencyFactory

from dataactcore.config import CONFIG_BROKER
from dataactcore.scripts.pipeline import load_federal_hierarchy
from dataactcore.models.domainModels import Office, ExternalDataType


def mock_request_to_fh(url):
    """ Simply mocking the federal hierarchy endpoint """
    # the file includes all the levels (which is usually split up by each individual call)
    # we're just going to filter out records at the level it's asking for
    level = re.findall(r'.*level=(\d).*', url)[0]

    fake_json_path = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'test_fh.json')
    with open(fake_json_path, 'r') as fake_json:
        fh_data = json.load(fake_json)[level]
    return fh_data


def test_pull_offices(monkeypatch, database):
    """ Test a simple pull of offices """
    monkeypatch.setattr('dataactcore.scripts.pipeline.load_federal_hierarchy.get_with_exception_hand',
                        mock_request_to_fh)
    monkeypatch.setattr('dataactcore.scripts.pipeline.load_federal_hierarchy.REQUESTS_AT_ONCE', 1)

    sess = database.session
    # Need to add these for 011 scenario, which conveniently aligns with the 001-014 codes in the test file
    frec_cgac = CGACFactory(cgac_code='011')
    matching_frec = FRECFactory(frec_code='0111', cgac=frec_cgac)
    frec_subtier = SubTierAgencyFactory(cgac=frec_cgac, frec=matching_frec, sub_tier_agency_code='0011')
    sess.add_all([frec_cgac, matching_frec, frec_subtier])

    metrics_json = {
        'missing_cgacs': [],
        'missing_subtier_codes': []
    }
    load_federal_hierarchy.pull_offices(sess, filename=None, update_db=True, pull_all=True,
                                        updated_date_from='2020-01-01', export_office=False, metrics=metrics_json)

    loaded_offices = list(sess.query(Office).all())

    # 14 are in the total levels but we're ignoring the 4 in the first two levels
    assert len(loaded_offices) == 10

    # All of them should have the same start date *except for the last inactive*
    for office_index in [0, 1, 2, 3, 4, 5, 6, 7, 9]:
        assert loaded_offices[office_index].effective_start_date == datetime.date(2021, 4, 13)
    # These offices in the test file were inactive and have effective end dates
    for office_index in [0, 2, 4, 6]:
        assert loaded_offices[office_index].effective_end_date == datetime.date(2021, 4, 14)
    # These offices in the test file were active and dont have an effective end date
    for office_index in [1, 3, 5, 7]:
        assert loaded_offices[office_index].effective_end_date is None
    # The last inactive office has *neither a start nor end date*, confirming our default values
    assert loaded_offices[8].effective_start_date == datetime.date(2000, 1, 1)
    assert loaded_offices[8].effective_end_date == datetime.date(2000, 1, 2)

    # These offices in the test file were *funding*
    for office_index in [0, 2, 4, 6, 8]:
        assert loaded_offices[office_index].contract_funding_office is True
        assert loaded_offices[office_index].financial_assistance_funding_office is True
        assert loaded_offices[office_index].contract_awards_office is False
        assert loaded_offices[office_index].financial_assistance_awards_office is False
    # These offices in the test file were *awarding*
    for office_index in [1, 3, 5, 7, 9]:
        assert loaded_offices[office_index].contract_awards_office is True
        assert loaded_offices[office_index].financial_assistance_awards_office is True
        assert loaded_offices[office_index].contract_funding_office is False
        assert loaded_offices[office_index].financial_assistance_funding_office is False

    # The special case where we have 011, check to see if it mapped to its FREC
    assert loaded_offices[6].agency_code == matching_frec.frec_code


def test_trim_nested_obj():
    """ Test trimming nested objects """
    test_json = [{' a ': ['1', '         2', '3     '], 'b': ['  2   ', '  3', '4']}]
    # note: it doesn't trim the keys, only the values
    result = [{' a ': ['1', '2', '3'], 'b': ['2', '3', '4']}]
    assert load_federal_hierarchy.trim_nested_obj(test_json) == result


def test_flatten_json():
    """ Test flattening jsons """
    test_json = {'a': [1, 2, 3], 'b': [2, 3, 4]}
    result = {'a_0': 1, 'a_1': 2, 'a_2': 3, 'b_0': 2, 'b_1': 3, 'b_2': 4}
    assert load_federal_hierarchy.flatten_json(test_json) == result

    test_json = [{'a': [1, 2, 3]}, {'b': [2, 3, 4]}]
    result = {'0_a_0': 1, '0_a_1': 2, '0_a_2': 3, '1_b_0': 2, '1_b_1': 3, '1_b_2': 4}
    assert load_federal_hierarchy.flatten_json(test_json) == result


def test_get_normalized_agency_code(database):
    """ Test get_normalized_agency_code to see if it's matching correctly """
    sess = database.session

    cgac = CGACFactory(cgac_code='098')
    cgac_subtier = SubTierAgencyFactory(cgac=cgac, sub_tier_agency_code='11AA')

    frec_cgac = CGACFactory(cgac_code='011')
    matching_frec = FRECFactory(frec_code='0222', cgac=frec_cgac)
    frec_subtier = SubTierAgencyFactory(cgac=frec_cgac, frec=matching_frec, sub_tier_agency_code='22AA')

    sess.add_all([cgac, cgac_subtier, frec_cgac, matching_frec, frec_subtier])

    # Test CGAC
    result = load_federal_hierarchy.get_normalized_agency_code(cgac.cgac_code, cgac_subtier.sub_tier_agency_code)
    assert result == cgac.cgac_code

    # Test FREC with matching subtier
    result = load_federal_hierarchy.get_normalized_agency_code(frec_cgac.cgac_code, frec_subtier.sub_tier_agency_code)
    assert result == matching_frec.frec_code

    # Test FREC with not matching subtier
    result = load_federal_hierarchy.get_normalized_agency_code('016', '33AA')
    assert result is None
