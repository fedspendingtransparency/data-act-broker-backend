from copy import deepcopy
from unittest.mock import Mock

from dataactcore.utils import fileE


_VALID_CONFIG = {'sam': {
    'wsdl': 'http://example.com', 'username': 'un', 'password': 'pass'}}


def test_configValid_empty(monkeypatch):
    monkeypatch.setattr(fileE, 'CONFIG_BROKER', {})
    assert not fileE.configValid()


def test_configValid_complete(monkeypatch):
    monkeypatch.setattr(fileE, 'CONFIG_BROKER', _VALID_CONFIG)
    assert fileE.configValid()


def test_configValid_missing_wsdl(monkeypatch):
    config = deepcopy(_VALID_CONFIG)
    del config['sam']['wsdl']
    monkeypatch.setattr(fileE, 'CONFIG_BROKER', config)
    assert not fileE.configValid()


def test_configValid_empty_username(monkeypatch):
    config = deepcopy(_VALID_CONFIG)
    config['sam']['username'] = ''
    monkeypatch.setattr(fileE, 'CONFIG_BROKER', config)
    assert not fileE.configValid()


def test_configValid_password_None(monkeypatch):
    config = deepcopy(_VALID_CONFIG)
    config['sam']['password'] = None
    monkeypatch.setattr(fileE, 'CONFIG_BROKER', config)
    assert not fileE.configValid()


def make_suds(duns, parent_duns, parent_name):
    sudsObj = Mock()
    sudsObj.entityIdentification.DUNS = duns
    parent = sudsObj.coreData.DUNSInformation.globalParentDUNS
    parent.DUNSNumber = parent_duns
    parent.legalBusinessName = parent_name
    return sudsObj


def test_sudsToRow_no_compensation():
    sudsObj = make_suds('A Duns', 'Par Duns', 'Par Name')
    sudsObj.coreData.listOfExecutiveCompensationInformation = ''
    row = fileE.sudsToRow(sudsObj)
    assert row == fileE.Row(
        'A Duns', 'Par Duns', 'Par Name', '', '', '', '', '', '', '', '',
        '', '')


def test_sudsToRow_too_few_compensation():
    sudsObj = make_suds('B Duns', 'Par DunsB', 'Par NameB')
    info = sudsObj.coreData.listOfExecutiveCompensationInformation

    middle = Mock(compensation=111.11)
    middle.name = 'Middle Person'   # "name" is a reserved word
    top = Mock(compensation=222.22)
    top.name = 'Top Person'
    bottom = Mock(compensation=0.0)
    bottom.name = 'Bottom Person'

    info.executiveCompensationDetail = [middle, top, bottom]
    row = fileE.sudsToRow(sudsObj)
    assert row == fileE.Row(
        'B Duns', 'Par DunsB', 'Par NameB',
        'Top Person', 222.22,
        'Middle Person', 111.11,
        'Bottom Person', 0.0,
        '', '', '', '')     # fills out to 5 pairs


def test_sudsToRow_too_many_compensation():
    sudsObj = make_suds('B Duns', 'Par DunsB', 'Par NameB')
    info = sudsObj.coreData.listOfExecutiveCompensationInformation
    info.executiveCompensationDetail = [
        Mock(compensation=i*11.11) for i in range(1, 10)]
    for idx, person in enumerate(info.executiveCompensationDetail):
        # Can't do this in the constructor as "name" is a reserved word
        person.name = "Person {}".format(idx + 1)

    row = fileE.sudsToRow(sudsObj)
    assert row == fileE.Row(
        'B Duns', 'Par DunsB', 'Par NameB',
        'Person 9', 99.99, 'Person 8', 88.88, 'Person 7', 77.77,
        'Person 6', 66.66, 'Person 5', 55.55)


def test_retrieveRows(monkeypatch):
    """Mock out a response from the SAM API and spot check several of the
    components that built it up"""
    monkeypatch.setattr(fileE, 'CONFIG_BROKER', _VALID_CONFIG)
    mock_result = Mock(
        listOfEntities=Mock(
            entity=[
                Mock(
                    entityIdentification=Mock(DUNS='entity1'),
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
    mock_Client = Mock()
    mock_Client.return_value.service.getEntities.return_value = mock_result
    monkeypatch.setattr(fileE, 'Client', mock_Client)

    rows = fileE.retrieveRows(['duns1', 'duns2'])
    assert len(rows) == 2
    assert rows[0].AwardeeOrRecipientUniqueIdentifier == 'entity1'
    assert rows[0].HighCompOfficer1Amount == 234.56
    assert rows[0].HighCompOfficer5Amount == ''
    assert rows[1].UltimateParentUniqueIdentifier == 'parent2Duns'
    assert rows[1].UltimateParentLegalEntityName == 'parent2'

    # [0] for positional args
    call_params = mock_Client.return_value.service.getEntities.call_args[0]
    auth, search, params = call_params
    assert auth.userID == _VALID_CONFIG['sam']['username']
    assert auth.password == _VALID_CONFIG['sam']['password']
    assert search.DUNSList.DUNSNumber == ['duns1', 'duns2']
    assert params.coreData.value == 'Y'
