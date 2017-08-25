import pandas as pd

from dataactcore.models.domainModels import CGAC
from dataactvalidator.scripts import loadAgencies
from tests.unit.dataactcore.factories.domain import CGACFactory


def test_delete_missing_cgacs(database):
    sess = database.session
    cgacs = [CGACFactory(cgac_code=str(i)) for i in range(5)]
    sess.add_all(cgacs)
    sess.commit()

    models = {cgac.cgac_code: cgac for cgac in cgacs}
    new_data = pd.DataFrame([
        {'cgac_code': '0'}, {'cgac_code': '2'}, {'cgac_code': '4'}, {'cgac_code': 'something-else'}
    ])

    assert set(models.keys()) == {'0', '1', '2', '3', '4'}
    loadAgencies.delete_missing_cgacs(models, new_data)
    assert set(models.keys()) == {'0', '2', '4'}

    assert {'0', '2', '4'} == {res.cgac_code for res in sess.query(CGAC.cgac_code)}


def test_update_cgacs(database):
    sess = database.session
    cgacs = [CGACFactory(cgac_code=str(i), agency_name=str(i) * 5) for i in range(2)]
    sess.add_all(cgacs)
    sess.commit()

    models = {cgac.cgac_code: cgac for cgac in cgacs}
    new_data = pd.DataFrame([
        {'cgac_code': '0', 'agency_name': 'other', 'agency_abbreviation': 'other'},
        {'cgac_code': '1', 'agency_name': '11111', 'agency_abbreviation': '11111'},
        {'cgac_code': 'something-else', 'agency_name': 'new_agency', 'agency_abbreviation': "new_agency"}
    ])

    assert models['0'].agency_name == '00000'
    assert models['1'].agency_name == '11111'
    assert 'something-else' not in models

    loadAgencies.update_cgacs(models, new_data)
    assert models['0'].agency_name == 'other (other)'
    assert models['1'].agency_name == '11111 (11111)'
    assert models['something-else'].agency_name == 'new_agency (new_agency)'
