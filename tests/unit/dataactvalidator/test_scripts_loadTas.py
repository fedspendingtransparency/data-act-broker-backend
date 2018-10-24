from csv import DictWriter
from datetime import date
from unittest.mock import Mock

import pandas as pd

from dataactcore.models.domainModels import TAS_COMPONENTS, TASLookup
from dataactvalidator.scripts import load_tas
from tests.unit.dataactcore.factories.domain import TASFactory


def write_then_read_tas(tmpdir, *rows):
    """ Helper function to write the provided rows to a CSV, then read them in ia `load_tas.clean_tas` """
    csv_file = tmpdir.join("cars_tas.csv")
    with open(str(csv_file), 'w') as f:
        writer = DictWriter(
            f, ['ACCT_NUM', 'ATA', 'AID', 'A', 'BPOA', 'EPOA', 'MAIN', 'SUB', 'FINANCIAL_INDICATOR_TYPE2',
                'DT_TM_ESTAB', 'DT_END', 'fr_entity_description', 'fr_entity_type', 'gwa_tas_name', 'agency_aid',
                'agency_name', 'admin_org', 'admin_org_name', 'function_code', 'function_description',
                'sub_function_code', 'sub_function_description']
        )
        writer.writeheader()
        for row in rows:
            data = {key: '' for key in writer.fieldnames}
            data.update(row)
            writer.writerow(data)

    return load_tas.clean_tas(str(csv_file))


def test_clean_tas_multiple(tmpdir):
    """ Happy path test that clean_tas will correctly read in a written CSV as a pandas dataframe """
    results = write_then_read_tas(
        tmpdir,
        {'ACCT_NUM': '6', 'ATA': 'aaa', 'AID': 'bbb', 'A': 'ccc',
         'BPOA': 'ddd', 'EPOA': 'eee', 'MAIN': 'ffff', 'SUB': 'ggg',
         'DT_END': '', 'DT_TM_ESTAB': '10/1/1987  12:00:00 AM',
         'fr_entity_description': 'abcd', 'fr_entity_type': '1234',
         'gwa_tas_name': 'name_6', 'agency_aid': 'bbb', 'agency_name': 'agency_name_6'},
        {'ACCT_NUM': '12345', 'ATA': '111', 'AID': '222', 'A': '333',
         'BPOA': '444', 'EPOA': '555', 'MAIN': '6666', 'SUB': '777',
         'DT_END': '12/22/2016  12:00:00 AM', 'DT_TM_ESTAB': '10/1/2008  12:00:00 AM',
         'fr_entity_description': 'xyz', 'fr_entity_type': 'AB12', 'admin_org': 'admin_org_12345',
         'admin_org_name': 'admin_org_name_12345', 'function_code': '800', 'function_description': '800_desc',
         'sub_function_code': '500', 'sub_function_description': '500_desc'}
    )

    assert results['account_num'].tolist() == [6, 12345]
    assert results['allocation_transfer_agency'].tolist() == ['aaa', '111']
    assert results['agency_identifier'].tolist() == ['bbb', '222']
    assert results['availability_type_code'].tolist() == ['ccc', '333']
    assert results['beginning_period_of_availa'].tolist() == ['ddd', '444']
    assert results['ending_period_of_availabil'].tolist() == ['eee', '555']
    assert results['main_account_code'].tolist() == ['ffff', '6666']
    assert results['sub_account_code'].tolist() == ['ggg', '777']
    assert results['internal_start_date'].tolist() == ['10/1/1987  12:00:00 AM', '10/1/2008  12:00:00 AM']
    assert results['internal_end_date'].tolist() == [None, '12/22/2016  12:00:00 AM']
    assert results['fr_entity_description'].tolist() == ['abcd', 'xyz']
    assert results['fr_entity_type'].tolist() == ['1234', 'AB12']
    assert results['account_title'].tolist() == ['name_6', None]
    assert results['reporting_agency_aid'].tolist() == ['bbb', None]
    assert results['reporting_agency_name'].tolist() == ['agency_name_6', None]
    assert results['budget_bureau_code'].tolist() == [None, 'admin_org_12345']
    assert results['budget_bureau_name'].tolist() == [None, 'admin_org_name_12345']
    assert results['budget_function_code'].tolist() == [None, '800']
    assert results['budget_function_title'].tolist() == [None, '800_desc']
    assert results['budget_subfunction_code'].tolist() == [None, '500']
    assert results['budget_subfunction_title'].tolist() == [None, '500_desc']


def test_clean_tas_space_nulls(tmpdir):
    """ Verify that spaces are converted into `None`s """
    results = write_then_read_tas(tmpdir, {'BPOA': '', 'EPOA': ' ', 'A': '   ', 'DT_END': '',
                                           'DT_TM_ESTAB': '10/1/2008  12:00:00 AM'})
    assert results['beginning_period_of_availa'][0] is None
    assert results['ending_period_of_availabil'][0] is None
    assert results['availability_type_code'][0] is None


def test_update_tas_lookups(database, monkeypatch):
    """ Verify that TAS with the same account_num can be modified, that we "close" any non-present TASes, and that we
    add new entries """
    sess = database.session
    existing_tas_entries = [
        # TAS present in both csv and db
        TASFactory(account_num=222, **{field: 'still-active' for field in TAS_COMPONENTS}),
        # Example of TAS being modified
        TASFactory(account_num=333, agency_identifier='to-close-1'),
        # Example unrelated to anything of these entries
        TASFactory(account_num=444, agency_identifier='to-close-2'),
        # Example of an existing, closed TAS
        TASFactory(account_num=555, agency_identifier='already-closed', internal_end_date=date(2015, 2, 2))
    ]
    sess.add_all(existing_tas_entries)
    sess.commit()

    incoming_tas_data = pd.DataFrame(
        columns=('account_num',) + TAS_COMPONENTS + ('internal_start_date', 'internal_end_date'),
        data=[
            [111] + ['new-entry-1'] * len(TAS_COMPONENTS)+[date(2015, 2, 2), None],
            [222] + ['still-active'] * len(TAS_COMPONENTS)+[date(2015, 2, 2), date(2016, 5, 2)],
            [333] + ['new-entry-2'] * len(TAS_COMPONENTS)+[date(2015, 2, 2), None],
        ]
    )
    monkeypatch.setattr(load_tas, 'clean_tas', Mock(return_value=incoming_tas_data))

    # Initial state
    assert sess.query(TASLookup).count() == 4

    load_tas.update_tas_lookups(sess, 'file-name-ignored-due-to-mock')

    # Post-"import" state
    results = sess.query(TASLookup).order_by(TASLookup.account_num, TASLookup.agency_identifier).all()
    assert len(results) == 5
    t111, t222, t333, t444, t555 = results

    assert t111.account_num == 111
    assert t111.internal_end_date is None               # active, new entry
    assert t111.agency_identifier == 'new-entry-1'

    assert t222.account_num == 222
    assert t222.internal_end_date == date(2016, 5, 2)     # newly closed based on new data
    assert t222.agency_identifier == 'still-active'

    assert t333.account_num == 333
    assert t333.internal_end_date is None           # active, continuing
    assert t333.agency_identifier == 'new-entry-2'

    assert t444.account_num == 444
    assert t444.internal_end_date is None            # active, continuing
    assert t444.agency_identifier == 'to-close-2'

    assert t555.account_num == 555
    assert t555.internal_end_date == date(2015, 2, 2)   # closed previously
    assert t555.agency_identifier == 'already-closed'


def test_only_fill_missing(database, monkeypatch):
    """ Verify that TAS with the same account_num can be modified, that we "close" any non-present TASes, and that we
        add new entries """
    sess = database.session

    blank_tas_fields = ['account_title', 'budget_bureau_code', 'budget_bureau_name', 'budget_function_code',
                        'budget_function_title', 'budget_subfunction_code', 'budget_subfunction_title',
                        'reporting_agency_aid', 'reporting_agency_name']

    existing_tas_entries = [
        # TAS to be filled in
        TASFactory(account_num=222, agency_identifier='to-close-2', **{field: None for field in blank_tas_fields}),
        # TAS to be untouched
        TASFactory(account_num=333, agency_identifier='to-close-3',
                   **{field: 'populated-333' for field in blank_tas_fields}),
    ]
    sess.add_all(existing_tas_entries)
    sess.commit()

    incoming_tas_data = pd.DataFrame(
        columns=('account_num',) + tuple(blank_tas_fields) + ('agency_identifier',),
        data=[
            [111] + ['populated-111'] * len(blank_tas_fields) + ['to-close-4'],
            [222] + ['populated-222'] * len(blank_tas_fields) + ['to-close-5'],

            [333] + ['populated-333'] * len(blank_tas_fields) + ['to-close-6'],
        ]
    )
    monkeypatch.setattr(load_tas, 'clean_tas', Mock(return_value=incoming_tas_data))

    # Initial state
    assert sess.query(TASLookup).count() == 2

    load_tas.update_tas_lookups(sess, 'file-name-ignored-due-to-mock', update_missing=[222])

    # Post-"import" state
    results = sess.query(TASLookup).order_by(TASLookup.account_num).all()
    assert len(results) == 2
    t222, t333 = results

    assert t222.account_num == 222
    assert t222.agency_identifier == 'to-close-2'
    for tas_field in blank_tas_fields:
        assert getattr(t222, tas_field) == 'populated-222'

    assert t333.account_num == 333
    assert t333.agency_identifier == 'to-close-3'
    for tas_field in blank_tas_fields:
        assert getattr(t333, tas_field) == 'populated-333'
