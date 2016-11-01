from csv import DictWriter
from datetime import date
from unittest.mock import Mock

from freezegun import freeze_time
import pandas as pd
import pytest

from dataactcore.models.domainModels import TASLookup
from dataactvalidator.scripts import loadTas
from tests.unit.dataactcore.factories.domain import TASFactory


def write_then_read_tas(tmpdir, *rows):
    """Helper function to write the provided rows to a CSV, then read them in
    via `loadTas.cleanTas`"""
    csv_file = tmpdir.join("cars_tas.csv")
    with open(str(csv_file), 'w') as f:
        writer = DictWriter(
            f, ['ACCT_NUM', 'ATA', 'AID', 'A', 'BPOA', 'EPOA', 'MAIN', 'SUB']
        )
        writer.writeheader()
        for row in rows:
            data = {key: '' for key in writer.fieldnames}
            data.update(row)
            writer.writerow(data)

    return loadTas.cleanTas(str(csv_file))


def test_cleanTas_multiple(tmpdir):
    """Happy path test that cleanTas will correctly read in a written CSV as a
    pandas dataframe"""
    results = write_then_read_tas(
        tmpdir,
        {'ACCT_NUM': '6', 'ATA': 'aaa', 'AID': 'bbb', 'A': 'ccc',
         'BPOA': 'ddd', 'EPOA': 'eee', 'MAIN': 'ffff', 'SUB': 'ggg'},
        {'ACCT_NUM': '12345', 'ATA': '111', 'AID': '222', 'A': '333',
         'BPOA': '444', 'EPOA': '555', 'MAIN': '6666', 'SUB': '777'}
    )
    assert results['tas_id'].tolist() == [6, 12345]
    assert results['allocation_transfer_agency'].tolist() == ['aaa', '111']
    assert results['agency_identifier'].tolist() == ['bbb', '222']
    assert results['availability_type_code'].tolist() == ['ccc', '333']
    assert results['beginning_period_of_availability'].tolist() == [
        'ddd', '444']
    assert results['ending_period_of_availability'].tolist() == ['eee', '555']
    assert results['main_account_code'].tolist() == ['ffff', '6666']
    assert results['sub_account_code'].tolist() == ['ggg', '777']


def test_cleanTas_space_nulls(tmpdir):
    """Verify that spaces are converted into `None`s"""
    results = write_then_read_tas(
        tmpdir, {'BPOA': '', 'EPOA': ' ', 'A': '   '})
    assert results['beginning_period_of_availability'][0] is None
    assert results['ending_period_of_availability'][0] is None
    assert results['availability_type_code'][0] is None


def test_updateTASLookups(database, monkeypatch):
    sess = database.session
    monkeypatch.setattr(
        loadTas, 'cleanTas', Mock(return_value=pd.DataFrame({
            'availability_type_code': ['0', '1', '2'],
            'tas_id': [111, 222, 333],
            'agency_identifier': ['0', '1', '2'],
            'allocation_transfer_agency': ['0', '1', '2'],
            'beginning_period_of_availability': ['0', '1', '2'],
            'ending_period_of_availability': ['0', '1', '2'],
            'main_account_code': ['0', '1', '2'],
            'sub_account_code': ['0', '1', '2'],
        }))
    )

    sess.add(TASFactory(tas_id=222))
    sess.add(TASFactory(tas_id=444, agency_identifier='other'))
    sess.commit()

    results = sess.query(TASLookup).order_by(TASLookup.tas_id).all()
    assert len(results) == 2
    sess.invalidate()

    loadTas.updateTASLookups('file-name-ignored-due-to-mock')

    results = sess.query(TASLookup).order_by(TASLookup.tas_id).all()
    assert len(results) == 3    # there is no 444
    assert results[0].tas_id == 111
    assert results[0].agency_identifier == '0'
    assert results[1].tas_id == 222
    assert results[1].agency_identifier == '1'  # replaces previous value
    assert results[2].tas_id == 333
    assert results[2].agency_identifier == '2'


def test_add_start_date_blank(database):
    """We backdate all of the TASLookups when there aren't any in the
    database"""
    assert database.session.query(TASLookup).count() == 0
    data = pd.DataFrame({'dummy': ['data']})
    loadTas.add_start_date(data)

    assert data['internal_start_date'][0] == date(2015, 1, 1)


@pytest.mark.parametrize("today_date,fiscal_date", [
    ('2016-01-01', date(2016, 1, 1)),
    ('2016-02-01', date(2016, 1, 1)),
    ('2016-03-22', date(2016, 1, 1)),
    ('2016-04-10', date(2016, 4, 1)),
    ('2016-08-08', date(2016, 7, 1)),
    ('2016-12-31', date(2016, 10, 1)),
])
def test_add_start_date_fiscal_year(database, today_date, fiscal_date):
    """We should be calculating the correct beginning of a fiscal year"""
    database.session.add(TASFactory())
    database.session.commit()

    data = pd.DataFrame({'dummy': ['data']})
    with freeze_time(today_date):
        loadTas.add_start_date(data)

    assert data['internal_start_date'][0] == fiscal_date
