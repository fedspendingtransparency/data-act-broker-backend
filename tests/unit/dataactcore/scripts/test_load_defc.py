import os
import numpy as np
import pandas as pd
import re

from dataactcore.config import CONFIG_BROKER
from dataactcore.scripts.setup import load_defc
from dataactcore.models.domainModels import DEFC, ExternalDataLoadDate


def mock_request_to_govinfo(url):
    """ Simply mocking the govinfo endpoint """
    govinfo_data = {}
    congress, law = re.findall(r'PLAW-(\d+)publ(\d+)', url)[0]
    if congress != '0':
        govinfo_data = {
            'title': f'Public Law {congress} - {law} - Test `Short Title` \"{congress}-{law}\"',
            'dcMD': {
                'dateIssued': f"2{congress}-01-02"
            },
            'download': {
                'pdflink': f'//test-pdf-link.gov/{congress}/{law}/{congress}-{law}.pdf'
            }
        }
    return govinfo_data


def test_derive_pl_data(monkeypatch):
    """ Test derive_pls_data ensuring it is derived correctly """
    monkeypatch.setattr('dataactcore.scripts.setup.load_defc.get_with_exception_hand', mock_request_to_govinfo)

    short_title, url, date_approved = load_defc.derive_pl_data('123-456')
    assert short_title == 'Test Short Title 123-456'
    assert url == 'http://test-pdf-link.gov/123/456/123-456.pdf'
    assert date_approved == '01/02/23'

    short_title, url, date_approved = load_defc.derive_pl_data('0-123')
    assert short_title == ''
    assert url == ''
    assert date_approved == ''


def test_derive_pls_data(monkeypatch):
    """ Test derive_pls_data ensuring they are derived correctly """
    monkeypatch.setattr('dataactcore.scripts.setup.load_defc.get_with_exception_hand', mock_request_to_govinfo)

    expected_series = pd.Series({
        'Public Law': 'Nonemergency PLs 123-456, 789-12, 0-111',
        'Public Laws': ['Non-emergency P.L. 123-456', 'Non-emergency P.L. 789-12', 'Non-emergency P.L. 0-111'],
        'Public Law Short Title': ['Test Short Title 123-456', 'Test Short Title 789-12', ''],
        'URLs': ['http://test-pdf-link.gov/123/456/123-456.pdf', 'http://test-pdf-link.gov/789/12/789-12.pdf', ''],
        'Earliest Public Law Enactment Date': '01/02/23'
    })
    pd.testing.assert_series_equal(load_defc.derive_pls_data('Nonemergency PLs 123-456, 789-12, 0-111'),
                                   expected_series)

    expected_series = pd.Series({
        'Public Law': 'Wildfire Suppression PLs 123-456, 789-12, 0-111',
        'Public Laws': ['Wildfire Suppression P.L. 123-456', 'Wildfire Suppression P.L. 789-12',
                        'Wildfire Suppression P.L. 0-111'],
        'Public Law Short Title': ['Test Short Title 123-456', 'Test Short Title 789-12', ''],
        'URLs': ['http://test-pdf-link.gov/123/456/123-456.pdf', 'http://test-pdf-link.gov/789/12/789-12.pdf', ''],
        'Earliest Public Law Enactment Date': '01/02/23'
    })

    pd.testing.assert_series_equal(load_defc.derive_pls_data('Wildfire Suppression PLs 123-456, 789-12, 0-111'),
                                   expected_series)


def test_apply_defc_derivations(monkeypatch):
    """ Test apply_defc_derivations ensuring they are added to the dataframe """
    monkeypatch.setattr('dataactcore.scripts.setup.load_defc.get_with_exception_hand', mock_request_to_govinfo)

    test_df = pd.DataFrame({
        'DEFC': ['L', '1'],
        'Public Law': ['Nonemergency PLs 123-456, 789-12, 0-111', 'Wildfire Suppression PLs 123-456, 789-12, 0-111']
    })

    expected_df = pd.DataFrame({
        'DEFC': ['L', '1'],
        'Public Law': [['Non-emergency P.L. 123-456', 'Non-emergency P.L. 789-12', 'Non-emergency P.L. 0-111'],
                       ['Wildfire Suppression P.L. 123-456', 'Wildfire Suppression P.L. 789-12',
                        'Wildfire Suppression P.L. 0-111']],
        'Public Law Short Title': [['Test Short Title 123-456', 'Test Short Title 789-12', ''],
                                   ['Test Short Title 123-456', 'Test Short Title 789-12', '']],
        'URLs': [['http://test-pdf-link.gov/123/456/123-456.pdf', 'http://test-pdf-link.gov/789/12/789-12.pdf', ''],
                 ['http://test-pdf-link.gov/123/456/123-456.pdf', 'http://test-pdf-link.gov/789/12/789-12.pdf', '']],
        'Earliest Public Law Enactment Date': ['01/02/23', '01/02/23'],
        'Group Name': ['covid_19', 'infrastructure'],
        'Is Valid': [True, True]
    })
    pd.testing.assert_frame_equal(load_defc.apply_defc_derivations(test_df), expected_df)


def test_add_defc_outliers():
    """ Test add_defc_outliers ensuring they are added to the dataframe """
    test_df = pd.DataFrame({
        'DEFC': [],
        'Public Law': [],
        'Public Law Short Title': [],
        'URLs': [],
        'Earliest Public Law Enactment Date': [],
        'Group Name': [],
        'Is Valid': []
    })
    test_df['Is Valid'] = test_df['Is Valid'].astype('bool')
    covid_defcs = load_defc.GROUP_MAPPINGS['covid_19']
    defc_9_title = f"DEFC of '9' Indicates that the data for this row is not related to a COVID-19 P.L." \
                   f" (DEFC not one of the following: {covid_defcs}), but that the agency has declined to specify" \
                   f" which other DEFC (or combination of DEFCs, in the case that the money hasn't been split out" \
                   f" like it would be with a specific DEFC value) applies." \
                   f" This code was discontinued on July 13, 2021."
    qqq_title = 'Excluded from tracking (uses non-emergency/non-disaster designated appropriations)'
    expected_df = pd.DataFrame({
        'DEFC': ['9', 'QQQ'],
        'Public Law': [[defc_9_title], [qqq_title]],
        'Public Law Short Title': [[defc_9_title], [qqq_title]],
        'URLs': [[], []],
        'Earliest Public Law Enactment Date': [np.nan, np.nan],
        'Group Name': [np.nan, np.nan],
        'Is Valid': [False, True]
    })
    pd.testing.assert_frame_equal(load_defc.add_defc_outliers(test_df), expected_df)


def test_load_defc(database, monkeypatch):
    """ Test actually loading the defc data """
    monkeypatch.setattr(load_defc, 'CONFIG_BROKER', {'use_aws': False})
    monkeypatch.setattr('dataactcore.scripts.setup.load_defc.get_with_exception_hand', mock_request_to_govinfo)

    sess = database.session

    base_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config")
    load_defc.load_defc(base_path)

    assert sess.query(DEFC).count() > 0
    assert sess.query(ExternalDataLoadDate).filter_by(external_data_type_id=20).count() > 0
