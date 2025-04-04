import os

from unittest.mock import Mock

from dataactcore.config import CONFIG_BROKER
from dataactcore.scripts.ad_hoc import load_full_sam_subaward_dump
from dataactcore.models.fsrs import SAMSubcontract, SAMSubgrant


def test_load_full_sam_subaward_dump_assistance(database, monkeypatch):
    """ Test loading the assistance data file """
    sess = database.session
    filename = 'SAM_Subaward_Bulk_Import_Assistance.csv'
    monkeypatch.setattr('dataactcore.scripts.ad_hoc.load_full_sam_subaward_dump.get_full_dump_filepath',
                        Mock(return_value=os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'fake_sam_files',
                                                       'subaward', filename)))
    load_full_sam_subaward_dump.load_full_dump_file(sess, 'assistance', {'assistance_subawards': 0})

    results = sess.query(SAMSubgrant).all()

    assert len(results) == 1
    assert results[0].uei == 'VENDORUEI'
    assert results[0].award_amount == '27354.55'
    assert len(results[0].business_types_codes) == 2
    assert 'XX' in results[0].business_types_codes


def test_load_full_sam_subaward_dump_contracts(database, monkeypatch):
    """ Test loading the contracts data file """
    sess = database.session
    filename = 'SAM_Subaward_Bulk_Import_Contracts.csv'
    monkeypatch.setattr('dataactcore.scripts.ad_hoc.load_full_sam_subaward_dump.get_full_dump_filepath',
                        Mock(return_value=os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'fake_sam_files',
                                                       'subaward', filename)))
    load_full_sam_subaward_dump.load_full_dump_file(sess, 'contract', {'contract_subawards': 0})

    results = sess.query(SAMSubcontract).all()

    assert len(results) == 1
    assert results[0].uei == 'ABCDEFGHIJK'
    assert results[0].award_amount == '69693.47'
