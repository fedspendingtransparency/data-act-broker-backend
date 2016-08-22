from unittest.mock import Mock

from dataactvalidator import fsrs


def newClient_call_args(monkeypatch, **config):
    """Set up and create a newClient request with the provided config. Return
    the arguments which were sent to the suds client"""
    mock_client = Mock()
    monkeypatch.setattr(fsrs, 'Client', mock_client)
    config = {'fsrs_service': config}
    monkeypatch.setattr(fsrs, 'CONFIG_BROKER', config)
    fsrs.newClient()
    return mock_client.call_args[1]


def test_newClient_wsdl(monkeypatch):
    """newClient will strip trailing '?wsdl's"""
    call_args = newClient_call_args(
        monkeypatch, wsdl='https://example.com/some?wsdl')
    assert call_args['location'] == 'https://example.com/some'
    assert call_args['url'] == 'https://example.com/some?wsdl'


def test_newClient_no_wsdl(monkeypatch):
    """newClient won't modify urls without the trailing '?wsdl'"""
    url = 'https://example.com/no_wsdl'
    call_args = newClient_call_args(monkeypatch, wsdl=url)
    assert 'location' not in call_args
    assert call_args['url'] == url


def test_newClient_import_fix(monkeypatch):
    """newClient should set an 'ImportDoctor' derived from the wsdl"""
    call_args = newClient_call_args(
        monkeypatch, wsdl='https://rando-domain.gov/some/where?wsdl')
    doctor = call_args['doctor']
    assert 'xmlsoap' in doctor.imports[0].ns
    assert doctor.imports[0].filter.tns[0] == 'https://rando-domain.gov/'


def test_newClient_auth(monkeypatch):
    """newClient should add http auth if _both_ username and password are
    configured"""
    call_args = newClient_call_args(monkeypatch)
    assert 'transport' not in call_args

    call_args = newClient_call_args(monkeypatch, username='incomplete')
    assert 'transport' not in call_args

    call_args = newClient_call_args(monkeypatch, password='incomplete')
    assert 'transport' not in call_args

    call_args = newClient_call_args(monkeypatch, username='u1', password='p2')
    assert call_args['transport'].options.username == 'u1'
    assert call_args['transport'].options.password == 'p2'
