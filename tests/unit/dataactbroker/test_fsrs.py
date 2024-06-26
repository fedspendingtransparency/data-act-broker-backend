from datetime import date
from unittest.mock import Mock

import pytest
from suds import sudsobject

from dataactcore.models.fsrs import FSRSGrant, FSRSProcurement, FSRSSubcontract, FSRSSubgrant
from dataactbroker import fsrs
from tests.unit.dataactcore.factories.fsrs import (
    FSRSGrantFactory, FSRSProcurementFactory, FSRSSubcontractFactory, FSRSSubgrantFactory)


def new_client_call_args(monkeypatch, **config):
    """Set up and create a new_client request with the provided config. Return
    the arguments which were sent to the suds client"""
    mock_client = Mock()
    monkeypatch.setattr(fsrs, 'Client', mock_client)
    config = {'fsrs': {'subconfig': config}}
    monkeypatch.setattr(fsrs, 'CONFIG_BROKER', config)
    fsrs.new_client('subconfig')
    args, kwargs = mock_client.call_args
    return kwargs


def test_new_client_wsdl(monkeypatch):
    """new_client will strip trailing '?wsdl's"""
    call_args = new_client_call_args(monkeypatch, wsdl='https://example.com/some?wsdl')
    assert call_args['location'] == 'https://example.com/some'
    assert call_args['url'] == 'https://example.com/some?wsdl'


def test_new_client_no_wsdl(monkeypatch):
    """new_client won't modify urls without the trailing '?wsdl'"""
    url = 'https://example.com/no_wsdl'
    call_args = new_client_call_args(monkeypatch, wsdl=url)
    assert 'location' not in call_args
    assert call_args['url'] == url


def test_new_client_import_fix(monkeypatch):
    """new_client should set an 'ImportDoctor' derived from the wsdl"""
    call_args = new_client_call_args(monkeypatch, wsdl='https://rando-domain.gov/some/where?wsdl')
    doctor = call_args['doctor']
    assert 'xmlsoap' in doctor.imports[0].ns
    assert doctor.imports[0].filter.tns[0] == 'https://rando-domain.gov/'


def test_new_client_auth(monkeypatch):
    """new_client should add http auth if _both_ username and password are
    configured"""
    call_args = new_client_call_args(monkeypatch)
    assert 'transport' not in call_args

    call_args = new_client_call_args(monkeypatch, username='incomplete')
    assert 'transport' not in call_args

    call_args = new_client_call_args(monkeypatch, password='incomplete')
    assert 'transport' not in call_args

    call_args = new_client_call_args(monkeypatch, username='u1', password='p2')
    assert call_args['transport'].options.username == 'u1'
    assert call_args['transport'].options.password == 'p2'


def test_new_client_filters(monkeypatch):
    """new_client should add the ControlFilter and ZeroDateFilter plugins.
    Those plugins should work."""
    call_args = new_client_call_args(monkeypatch)
    assert 'plugins' in call_args
    assert len(call_args['plugins']) == 2

    input_value = b'Some \x01thing\x16here. Date: 0000-00-00 stuff'
    for plugin in call_args['plugins']:
        mock_context = Mock(reply=input_value)
        plugin.received(mock_context)   # mutates in place
        input_value = mock_context.reply
    assert input_value == b'Some  thing here. Date: 0001-01-01 stuff'


def test_soap_to_dict():
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

    assert fsrs.soap_to_dict(root) == expected


def test_flatten_soap_dict():
    """Spot check that data's imported to the proper fields"""
    obj = dict(
        uei_number='UeINuM', recovery_model_q1=True, bus_types=['a', 'b', 'c'],
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
        uei_number='UeINuM', recovery_model_q1=True, bus_types='a,b,c',
        company_address_city='CompanyCity',
        company_address_street=None, company_address_state=None, company_address_state_name=None,
        company_address_country=None, company_address_zip=None,
        company_address_district='CompanyDist',
        principle_place_street='PrincipleStreet',
        principle_place_city=None, principle_place_state=None, principle_place_state_name=None,
        principle_place_country=None, principle_place_zip=None,
        principle_place_district=None,
        top_paid_fullname_1='full1', top_paid_amount_1='1',
        top_paid_fullname_2='full2', top_paid_amount_2='2',
        top_paid_fullname_3='full3', top_paid_amount_3='3',
        top_paid_fullname_4='full4', top_paid_amount_4='4',
        top_paid_fullname_5='full5', top_paid_amount_5='5'
    )
    result = fsrs.flatten_soap_dict(
        simple_fields=('uei_number', 'recovery_model_q1'),
        address_fields=('company_address', 'principle_place'),
        comma_field='bus_types',
        soap_dict=obj)
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


def test_next_id_default(no_award_db):
    assert FSRSProcurement.next_id(no_award_db) == 0


def test_next_id(no_award_db):
    no_award_db.add_all([FSRSProcurementFactory(id=5), FSRSProcurementFactory(id=3), FSRSGrantFactory(id=2)])
    no_award_db.commit()

    assert 6 == FSRSProcurement.next_id(no_award_db)
    assert 3 == FSRSGrant.next_id(no_award_db)


def test_fetch_and_replace_batch_saves_data(no_award_db, monkeypatch):
    award1 = FSRSProcurementFactory()
    award1.subawards = [FSRSSubcontractFactory() for _ in range(4)]
    award2 = FSRSProcurementFactory()
    award2.subawards = [FSRSSubcontractFactory()]
    monkeypatch.setattr(fsrs, 'retrieve_batch', Mock(return_value=[award1, award2]))

    assert no_award_db.query(FSRSProcurement).count() == 0
    fsrs.fetch_and_replace_batch(no_award_db, fsrs.PROCUREMENT, id=award1.internal_id, min_id=True)
    assert no_award_db.query(FSRSProcurement).count() == 2
    assert no_award_db.query(FSRSSubcontract).count() == 5


def test_fetch_and_replace_batch_overrides_data(no_award_db, monkeypatch):
    def fetch_uei(award_id):
        test_result = no_award_db.query(FSRSGrant.uei_number).filter(
            FSRSGrant.id == award_id).one_or_none()
        if test_result:
            return test_result[0]
        return None

    award1 = FSRSGrantFactory(id=1, internal_id='12345', uei_number='To Be Replaced')
    award1.subawards = [FSRSSubgrantFactory() for _ in range(4)]
    award2 = FSRSGrantFactory(id=2, internal_id='54321', uei_number='Not Altered')
    award2.subawards = [FSRSSubgrantFactory()]
    monkeypatch.setattr(fsrs, 'retrieve_batch', Mock(return_value=[award1, award2]))

    fsrs.fetch_and_replace_batch(no_award_db, fsrs.GRANT, id=award1.internal_id, min_id=True)
    assert fetch_uei(1) == 'To Be Replaced'
    assert fetch_uei(2) == 'Not Altered'
    # 5 subawards, 4 from award1 and 1 from award2
    assert no_award_db.query(FSRSSubgrant).count() == 5

    no_award_db.expunge_all()   # Reset the model cache

    award3 = FSRSGrantFactory(id=3, internal_id='12345', uei_number='Replaced')
    award3.subawards = [FSRSSubgrantFactory() for _ in range(2)]
    monkeypatch.setattr(fsrs, 'retrieve_batch', Mock(return_value=[award3]))
    fsrs.fetch_and_replace_batch(no_award_db, fsrs.GRANT, id=award3.internal_id, min_id=True)
    assert fetch_uei(1) is None
    assert fetch_uei(2) == 'Not Altered'
    assert fetch_uei(3) == 'Replaced'
    # 3 subawards, 4 from award1/award3 and 1 from award2
    assert no_award_db.query(FSRSSubgrant).count() == 3
