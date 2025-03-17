import pytest
import os

from dataactcore.config import CONFIG_BROKER
from dataactcore.scripts.ad_hoc import load_full_sam_subaward_dump
from dataactcore.models.domainModels import CGAC, FREC, SubTierAgency, ExternalDataType, ExternalDataLoadDate


def test_load_full_sam_subaward_dump_assistance(database, monkeypatch):
    """ Test loading the assistance data file """
    filename = 'SAM_Subward_Bulk_Import_Assistance.csv'
    monkeypatch.setattr('dataactcore.scripts.ad_hoc.load_full_sam_subaward_dump.get_full_dump_filepath',
                        os.path.join(CONFIG_BROKER['path'], 'tests', 'data', 'config', filename))

    assert True


def test_load_full_sam_subaward_dump_contracts(database, monkeypatch):
    """ Test loading the contracts data file """
    filename = 'SAM_Subward_Bulk_Import_Contracts.csv'
    monkeypatch.setattr('dataactcore.scripts.ad_hoc.load_full_sam_subaward_dump.get_full_dump_filepath',
                        os.path.join(CONFIG_BROKER['path'], 'tests', 'data', 'config', filename))

    assert True
