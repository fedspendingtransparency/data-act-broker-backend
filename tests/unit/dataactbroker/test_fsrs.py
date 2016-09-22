from datetime import date
from unittest.mock import Mock

import pytest
from suds import sudsobject

from dataactcore.models.fsrs import (
    FSRSGrant, FSRSProcurement, FSRSSubcontract, FSRSSubgrant)
from dataactbroker import fsrs
from tests.unit.dataactcore.factories.fsrs import (
    FSRSGrantFactory, FSRSProcurementFactory, FSRSSubcontractFactory,
    FSRSSubgrantFactory)


def newClient_call_args(monkeypatch, **config):
    """Set up and create a newClient request with the provided config. Return
    the arguments which were sent to the suds client"""
    mock_client = Mock()
    monkeypatch.setattr(fsrs, 'Client', mock_client)
    config = {'fsrs': {'subconfig': config}}
    monkeypatch.setattr(fsrs, 'CONFIG_BROKER', config)
    fsrs.newClient('subconfig')
    args, kwargs = mock_client.call_args
    return kwargs


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


def test_newClient_controlFilter(monkeypatch):
    """newClient should add the ControlFilter plugin. It should work."""
    call_args = newClient_call_args(monkeypatch)
    assert 'plugins' in call_args
    assert len(call_args['plugins']) == 1

    mock_context = Mock(reply=b'Some \x01thing\x16here')
    control_filter = call_args['plugins'][0]
    control_filter.received(mock_context)   # mutates in place
    assert mock_context.reply == b'Some  thing here'


def test_soap2Dict():
    today = date.today()

    root = sudsobject.Object()
    root.text = "Some text"
    root.num = 1234
    root.sub = sudsobject.Object()
    root.sub.date = today
    root.sub.name = "Sub value"
    root.children = [sudsobject.Object(), sudsobject.Object()]
    root.children[0].name = 'Obj 0'
    root.children[1].name = 'Obj 1'
    root.children[1].sub = sudsobject.Object()
    root.children[1].sub.name = "Obj 1's Sub"

    expected = dict(
        text="Some text", num=1234,
        sub=dict(date=today, name='Sub value'),
        children=[
            dict(name='Obj 0'),
            dict(name='Obj 1', sub=dict(name="Obj 1's Sub"))
        ]
    )

    assert fsrs.soap2Dict(root) == expected


def test_flattenSoapDict():
    """Spot check that data's imported to the proper fields"""
    obj = dict(
        duns='DuNs', recovery_model_q1=True, bus_types=['a', 'b', 'c'],
        dropped='field', another_dropped=['l', 'i', 's', 't'],
        company_address=dict(city='CompanyCity', district='CompanyDist'),
        principle_place=dict(street='PrincipleStreet'),
        top_pay_employees=dict(
            employee_1=dict(fullname='full1', amount='1'),
            employee_2=dict(fullname='full2', amount='2'),
            employee_3=dict(fullname='full3', amount='3'),
            employee_4=dict(fullname='full4', amount='4'),
            employee_5=dict(fullname='full5', amount='5')
        )
    )
    expected = dict(
        duns='DuNs', recovery_model_q1=True, bus_types='a,b,c',
        company_address_city='CompanyCity',
        company_address_street=None, company_address_state=None,
        company_address_country=None, company_address_zip=None,
        company_address_district='CompanyDist',
        principle_place_street='PrincipleStreet',
        principle_place_city=None, principle_place_state=None,
        principle_place_country=None, principle_place_zip=None,
        principle_place_district=None,
        top_paid_fullname_1='full1', top_paid_amount_1='1',
        top_paid_fullname_2='full2', top_paid_amount_2='2',
        top_paid_fullname_3='full3', top_paid_amount_3='3',
        top_paid_fullname_4='full4', top_paid_amount_4='4',
        top_paid_fullname_5='full5', top_paid_amount_5='5'
    )
    result = fsrs.flattenSoapDict(
        simpleFields=('duns', 'recovery_model_q1'),
        addressFields=('company_address', 'principle_place'),
        commaField='bus_types',
        soapDict=obj)
    assert result == expected


@pytest.fixture()
def no_award_db(database):
    sess = database.session
    sess.query(FSRSProcurement).delete(synchronize_session=False)
    sess.query(FSRSGrant).delete(synchronize_session=False)
    sess.commit()

    yield sess

    sess.query(FSRSProcurement).delete(synchronize_session=False)
    sess.query(FSRSGrant).delete(synchronize_session=False)
    sess.commit()


def test_nextId_default(no_award_db):
    assert FSRSProcurement.nextId(no_award_db) == 0


def test_nextId(no_award_db):
    no_award_db.add_all([FSRSProcurementFactory(id=5),
                         FSRSProcurementFactory(id=3),
                         FSRSGrantFactory(id=2)])
    no_award_db.commit()

    assert 6 == FSRSProcurement.nextId(no_award_db)
    assert 3 == FSRSGrant.nextId(no_award_db)


def test_fetchAndReplaceBatch_saves_data(no_award_db, monkeypatch):
    award1 = FSRSProcurementFactory()
    award1.subawards = [FSRSSubcontractFactory() for _ in range(4)]
    award2 = FSRSProcurementFactory()
    award2.subawards = [FSRSSubcontractFactory()]
    monkeypatch.setattr(fsrs, 'retrieveBatch',
                        Mock(return_value=[award1, award2]))

    assert no_award_db.query(FSRSProcurement).count() == 0
    fsrs.fetchAndReplaceBatch(no_award_db, fsrs.PROCUREMENT)
    assert no_award_db.query(FSRSProcurement).count() == 2
    assert no_award_db.query(FSRSSubcontract).count() == 5


def test_fetchAndReplaceBatch_overrides_data(no_award_db, monkeypatch):
    def fetch_duns(award_id):
        return no_award_db.query(FSRSGrant.duns).filter(
            FSRSGrant.id == award_id).one()[0]

    award1 = FSRSGrantFactory(id=1, duns='To Be Replaced')
    award1.subawards = [FSRSSubgrantFactory() for _ in range(4)]
    award2 = FSRSGrantFactory(id=2, duns='Not Altered')
    award2.subawards = [FSRSSubgrantFactory()]
    monkeypatch.setattr(fsrs, 'retrieveBatch',
                        Mock(return_value=[award1, award2]))

    fsrs.fetchAndReplaceBatch(no_award_db, fsrs.GRANT)
    assert fetch_duns(1) == 'To Be Replaced'
    assert fetch_duns(2) == 'Not Altered'
    # 5 subawards, 4 from award1 and 1 from award2
    assert no_award_db.query(FSRSSubgrant).count() == 5

    no_award_db.expunge_all()   # Reset the model cache

    award3 = FSRSGrantFactory(id=1, duns='Replaced')
    award3.subawards = [FSRSSubgrantFactory() for _ in range(2)]
    monkeypatch.setattr(fsrs, 'retrieveBatch', Mock(return_value=[award3]))
    fsrs.fetchAndReplaceBatch(no_award_db, fsrs.GRANT)
    assert fetch_duns(1) == 'Replaced'
    assert fetch_duns(2) == 'Not Altered'
    # 3 subawards, 4 from award1/award3 and 1 from award2
    assert no_award_db.query(FSRSSubgrant).count() == 3
