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
    assert results['account_num'].tolist() == [6, 12345]
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
    """Verify that TAS with the same account_num can be modified, that we
    "close" any non-present TASes, and that we add new entries"""
    sess = database.session
    existing_tas_entries = [
        # TAS present in both csv and db
        TASFactory(
            account_num=222,
            **{field: 'still-active' for field in loadTas._MATCH_FIELDS}),
        # Example of TAS being modified
        TASFactory(account_num=333, agency_identifier='to-close-1'),
        # Example unrelated to anything of these entries
        TASFactory(account_num=444, agency_identifier='to-close-2'),
        # Example of an existing, closed TAS
        TASFactory(account_num=555, agency_identifier='already-closed',
                   internal_end_date=date(2015, 2, 2))
    ]
    sess.add_all(existing_tas_entries)
    sess.commit()

    incoming_tas_data = pd.DataFrame(
        columns=('account_num',) + loadTas._MATCH_FIELDS,
        data=[
            [111] + ['new-entry-1'] * len(loadTas._MATCH_FIELDS),
            [222] + ['still-active'] * len(loadTas._MATCH_FIELDS),
            [333] + ['new-entry-2'] * len(loadTas._MATCH_FIELDS)
        ]
    )
    monkeypatch.setattr(loadTas, 'cleanTas',
                        Mock(return_value=incoming_tas_data))

    # Initial state
    assert sess.query(TASLookup).count() == 4

    loadTas.updateTASLookups('file-name-ignored-due-to-mock')

    # Post-"import" state
    results = sess.query(TASLookup).\
        order_by(TASLookup.account_num, TASLookup.agency_identifier).all()
    assert len(results) == 6
    t111, t222, t333_new, t333_old, t444, t555 = results

    assert t111.account_num == 111
    assert t111.internal_end_date == None               # active, new entry
    assert t111.agency_identifier == 'new-entry-1'

    assert t222.account_num == 222
    assert t222.internal_end_date is None               # active, continuing
    assert t222.agency_identifier == 'still-active'

    assert t333_old.account_num == 333
    assert t333_old.internal_end_date == date.today()   # closed today
    assert t333_old.agency_identifier == 'to-close-1'

    assert t333_new.account_num == 333
    assert t333_new.internal_end_date is None           # active, new entry
    assert t333_new.agency_identifier == 'new-entry-2'

    assert t444.account_num == 444
    assert t444.internal_end_date == date.today()       # closed today
    assert t444.agency_identifier == 'to-close-2'

    assert t555.account_num == 555
    assert t555.internal_end_date == date(2015, 2, 2)   # closed previously
    assert t555.agency_identifier == 'already-closed'


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
