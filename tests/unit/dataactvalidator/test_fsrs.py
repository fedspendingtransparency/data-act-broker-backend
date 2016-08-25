from datetime import date, datetime
from unittest.mock import Mock

from suds import sudsobject

from dataactvalidator import fsrs


def newClient_call_args(monkeypatch, **config):
    """Set up and create a newClient request with the provided config. Return
    the arguments which were sent to the suds client"""
    mock_client = Mock()
    monkeypatch.setattr(fsrs, 'Client', mock_client)
    config = {'fsrs_service': config}
    monkeypatch.setattr(fsrs, 'CONFIG_BROKER', config)
    fsrs.newClient()
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


def test_commonAttributes():
    """Spot check that data's imported to the proper fields"""
    obj = dict(
        duns='DuNs', recovery_model_q1=True, bus_types=['a', 'b', 'c'],
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
        company_address_district='CompanyDist',
        principle_place_street='PrincipleStreet',
        top_paid_fullname_1='full1', top_paid_amount_1='1',
        top_paid_fullname_2='full2', top_paid_amount_2='2',
        top_paid_fullname_3='full3', top_paid_amount_3='3',
        top_paid_fullname_4='full4', top_paid_amount_4='4',
        top_paid_fullname_5='full5', top_paid_amount_5='5'
    )
    result = fsrs.commonAttributes(obj)
    for key, value in expected.items():
        assert result.get(key) == value
    for key, value in result.items():
        assert expected.get(key) == value


def test_award2Model_subcon2Model(monkeypatch):
    monkeypatch.setattr(fsrs, 'commonAttributes', lambda _: {})
    now = datetime.now()
    award = dict(
        id=5, internal_id='iii', date_signed=now,
        subcontractors=[
            dict(subcontract_amount='45', unused_field='Unused'),
            dict(subcontract_num='67')
        ]
    )
    result = fsrs.award2Model(award)

    assert result.id == 5
    assert result.internal_id == 'iii'
    assert result.date_signed == now
    assert len(result.subawards) == 2
    assert result.subawards[0].subcontract_amount == '45'
    assert result.subawards[1].subcontract_num == '67'
