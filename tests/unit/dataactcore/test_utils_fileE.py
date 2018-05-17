from copy import deepcopy
from unittest.mock import Mock

from dataactcore.utils import fileE


_VALID_CONFIG = {'sam': {'wsdl': 'http://example.com', 'username': 'un', 'password': 'pass'}}


def test_config_valid_empty(monkeypatch):
    monkeypatch.setattr(fileE, 'CONFIG_BROKER', {})
    assert not fileE.config_valid()


def test_config_valid_complete(monkeypatch):
    monkeypatch.setattr(fileE, 'CONFIG_BROKER', _VALID_CONFIG)
    assert fileE.config_valid()


def test_config_valid_missing_wsdl(monkeypatch):
    config = deepcopy(_VALID_CONFIG)
    del config['sam']['wsdl']
    monkeypatch.setattr(fileE, 'CONFIG_BROKER', config)
    assert not fileE.config_valid()


def test_config_valid_empty_username(monkeypatch):
    config = deepcopy(_VALID_CONFIG)
    config['sam']['username'] = ''
    monkeypatch.setattr(fileE, 'CONFIG_BROKER', config)
    assert not fileE.config_valid()


def test_config_valid_password_none(monkeypatch):
    config = deepcopy(_VALID_CONFIG)
    config['sam']['password'] = None
    monkeypatch.setattr(fileE, 'CONFIG_BROKER', config)
    assert not fileE.config_valid()


def make_suds(duns, legal_business_name, parent_duns, parent_name):
    suds_obj = Mock()
    suds_obj.entityIdentification.DUNS = duns
    suds_obj.entityIdentification.legalBusinessName = legal_business_name
    parent = suds_obj.coreData.DUNSInformation.globalParentDUNS
    parent.DUNSNumber = parent_duns
    parent.legalBusinessName = parent_name
    return suds_obj


def test_suds_to_row_no_compensation():
    suds_obj = make_suds('A Duns', 'Legal Business Name', 'Par Duns', 'Par Name')
    suds_obj.coreData.listOfExecutiveCompensationInformation = ''
    row = fileE.suds_to_row(suds_obj)
    assert row == fileE.Row('A Duns', 'Legal Business Name',  'Par Duns', 'Par Name',
                            '', '', '', '', '', '', '', '', '', '')

    del suds_obj.coreData.listOfExecutiveCompensationInformation
    row = fileE.suds_to_row(suds_obj)
    assert row == fileE.Row('A Duns', 'Legal Business Name', 'Par Duns', 'Par Name',
                            '', '', '', '', '', '', '', '', '', '')


def test_suds_to_row_too_few_compensation():
    suds_obj = make_suds('B Duns', 'Legal Business Name', 'Par DunsB', 'Par NameB')
    info = suds_obj.coreData.listOfExecutiveCompensationInformation

    middle = Mock(compensation=111.11)
    middle.name = 'Middle Person'   # "name" is a reserved word
    top = Mock(compensation=222.22)
    top.name = 'Top Person'
    bottom = Mock(compensation=0.0)
    bottom.name = 'Bottom Person'

    info.executiveCompensationDetail = [middle, top, bottom]
    row = fileE.suds_to_row(suds_obj)
    assert row == fileE.Row(
        'B Duns', 'Legal Business Name', 'Par DunsB', 'Par NameB',
        'Top Person', 222.22,
        'Middle Person', 111.11,
        'Bottom Person', 0.0,
        '', '', '', '')     # fills out to 5 pairs


def test_suds_to_row_too_many_compensation():
    suds_obj = make_suds('B Duns', 'Legal Business Name', 'Par DunsB', 'Par NameB')
    info = suds_obj.coreData.listOfExecutiveCompensationInformation
    info.executiveCompensationDetail = [Mock(compensation=i * 11.11) for i in range(1, 10)]
    for idx, person in enumerate(info.executiveCompensationDetail):
        # Can't do this in the constructor as "name" is a reserved word
        person.name = "Person {}".format(idx + 1)

    row = fileE.suds_to_row(suds_obj)
    assert row == fileE.Row(
        'B Duns', 'Legal Business Name', 'Par DunsB', 'Par NameB',
        'Person 9', 99.99, 'Person 8', 88.88, 'Person 7', 77.77,
        'Person 6', 66.66, 'Person 5', 55.55)


def test_retrieve_rows(monkeypatch):
    """Mock out a response from the SAM API and spot check several of the
    components that built it up"""
    monkeypatch.setattr(fileE, 'CONFIG_BROKER', _VALID_CONFIG)
    mock_result = Mock(
        listOfEntities=Mock(
            entity=[
                Mock(
                    entityIdentification=Mock(DUNS='entity1', legalBusinessName='Legal Business Name'),
                    coreData=Mock(
                        listOfExecutiveCompensationInformation=Mock(
                            executiveCompensationDetail=[
                                Mock(compensation=123.45),
                                Mock(compensation=234.56)
                            ]
                        ),
                        DUNSInformation=Mock(
                            globalParentDUNS=Mock(
                                DUNSNumber='parent1Duns',
                                legalBusinessName='parent1'
                            )
                        )
                    )
                ),
                Mock(
                    entityIdentification=Mock(DUNS='entity2'),
                    coreData=Mock(
                        # Indicates no data
                        listOfExecutiveCompensationInformation='',
                        DUNSInformation=Mock(
                            globalParentDUNS=Mock(
                                DUNSNumber='parent2Duns',
                                legalBusinessName='parent2'
                            )
                        )
                    )
                ),
            ]
        )
    )
    mock_client = Mock()
    mock_client.return_value.service.getEntities.return_value = mock_result
    monkeypatch.setattr(fileE, 'Client', mock_client)

    rows = fileE.retrieve_rows(['duns1', 'duns2'])
    assert len(rows) == 2
    assert rows[0].AwardeeOrRecipientUniqueIdentifier == 'entity1'
    assert rows[0].AwardeeOrRecipientLegalEntityName == 'Legal Business Name'
    assert rows[0].HighCompOfficer1Amount == 234.56
    assert rows[0].HighCompOfficer5Amount == ''
    assert rows[1].UltimateParentUniqueIdentifier == 'parent2Duns'
    assert rows[1].UltimateParentLegalEntityName == 'parent2'

    # [0] for positional args
    call_params = mock_client.return_value.service.getEntities.call_args[0]
    auth, search, params = call_params
    assert auth.userID == _VALID_CONFIG['sam']['username']
    assert auth.password == _VALID_CONFIG['sam']['password']
    assert search.DUNSList.DUNSNumber == ['duns1', 'duns2']
    assert params.coreData.value == 'Y'
