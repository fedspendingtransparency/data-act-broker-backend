from copy import deepcopy
from unittest.mock import Mock

from dataactbroker.helpers import sam_wsdl_helper

_VALID_CONFIG = {'sam': {'wsdl': 'http://example.com', 'username': 'un', 'password': 'pass'}}


def test_config_valid_empty(monkeypatch):
    monkeypatch.setattr(sam_wsdl_helper, 'CONFIG_BROKER', {})
    assert not sam_wsdl_helper.config_valid()


def test_config_valid_complete(monkeypatch):
    monkeypatch.setattr(sam_wsdl_helper, 'CONFIG_BROKER', _VALID_CONFIG)
    assert sam_wsdl_helper.config_valid()


def test_config_valid_missing_wsdl(monkeypatch):
    config = deepcopy(_VALID_CONFIG)
    del config['sam']['wsdl']
    monkeypatch.setattr(sam_wsdl_helper, 'CONFIG_BROKER', config)
    assert not sam_wsdl_helper.config_valid()


def test_config_valid_empty_username(monkeypatch):
    config = deepcopy(_VALID_CONFIG)
    config['sam']['username'] = ''
    monkeypatch.setattr(sam_wsdl_helper, 'CONFIG_BROKER', config)
    assert not sam_wsdl_helper.config_valid()


def test_config_valid_password_none(monkeypatch):
    config = deepcopy(_VALID_CONFIG)
    config['sam']['password'] = None
    monkeypatch.setattr(sam_wsdl_helper, 'CONFIG_BROKER', config)
    assert not sam_wsdl_helper.config_valid()


def make_suds(duns, legal_business_name, parent_duns, parent_name):
    suds_obj = Mock()
    suds_obj.entityIdentification.DUNS = duns
    suds_obj.entityIdentification.legalBusinessName = legal_business_name
    parent = suds_obj.coreData.DUNSInformation.globalParentDUNS
    parent.DUNSNumber = parent_duns
    parent.legalBusinessName = parent_name
    return suds_obj
