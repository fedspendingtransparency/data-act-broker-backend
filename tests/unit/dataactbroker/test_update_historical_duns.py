import pandas as pd
import os

from dataactcore.config import CONFIG_BROKER
from dataactbroker.scripts import update_historical_duns
from dataactcore.models.domainModels import DUNS, HistoricDUNS


def test_remove_existing_duns(database):
    """ Testing the removing existing duns function"""
    sess = database.session
    # of the duns 000000001-000000009, half of them are in the database
    all_duns = ['00000000{}'.format(x) for x in range(0, 10)]
    existing_duns = all_duns[: 4]
    data = pd.DataFrame.from_dict({'awardee_or_recipient_uniqu': all_duns})
    for duns in existing_duns:
        sess.add(DUNS(awardee_or_recipient_uniqu=duns))
    sess.commit()

    # confirm that the dataframe returned only has half the duns
    expected_duns = list(set(existing_duns) ^ set(all_duns))
    new_df = update_historical_duns.remove_existing_duns(data, sess)
    assert sorted(expected_duns) == sorted(new_df['awardee_or_recipient_uniqu'].tolist())


def test_batch():
    """ Testing the batch function into chunks of 100 """
    full_list = list(range(0, 1000))
    initial_batch = list(range(0, 100))
    iteration = 0
    batch_size = 100
    for batch in update_historical_duns.batch(full_list, batch_size):
        expected_batch = [x+(batch_size*iteration) for x in initial_batch]
        assert expected_batch == batch
        iteration += 1
    assert iteration == 10


def mock_get_duns_props_from_sam(client, duns_list):
    """ Mock function for location_business data as we can't connect to the SAM service """
    columns = ['awardee_or_recipient_uniqu'] + list(update_historical_duns.props_columns.keys())
    results = pd.DataFrame(columns=columns)
    duns_mappings = {
        '000000001': {
            'awardee_or_recipient_uniqu': ['000000001'],
            'legal_business_name': ['Legal Name 1'],
            'dba_name': ['Name 1'],
            'ultimate_parent_unique_ide': ['999999999'],
            'ultimate_parent_legal_enti': ['Parent Legal Name 1'],
            'address_line_1': ['Test address 1'],
            'address_line_2': ['Test address 2'],
            'city': ['Test city'],
            'state': ['Test state'],
            'zip': ['Test zip'],
            'zip4': ['Test zip4'],
            'country_code': ['Test country'],
            'congressional_district': ['Test congressional district'],
            'business_types_codes': [['A', 'B', 'C']]
        },
        '000000002': {
            'awardee_or_recipient_uniqu': ['000000002'],
            'legal_business_name': ['Legal Name 2'],
            'dba_name': ['Name 2'],
            'ultimate_parent_unique_ide': ['999999998'],
            'ultimate_parent_legal_enti': ['Parent Legal Name 2'],
            'address_line_1': ['Other Test address 1'],
            'address_line_2': ['Other Test address 2'],
            'city': ['Other Test city'],
            'state': ['Other Test state'],
            'zip': ['Other Test zip'],
            'zip4': ['Other Test zip4'],
            'country_code': ['Other Test country'],
            'congressional_district': ['Other Test congressional district'],
            'business_types_codes': [['D', 'E', 'F']]
        }
    }
    for duns in duns_list:
        if duns in duns_mappings:
            results = results.append(pd.DataFrame(duns_mappings[duns]))
    return results


def test_update_duns_props(monkeypatch):
    """ Testing updating the duns props with both populated/blank data """
    monkeypatch.setattr('dataactcore.utils.parentDuns.get_duns_props_from_sam',
                        mock_get_duns_props_from_sam)
    duns_df = pd.DataFrame.from_dict({
        'awardee_or_recipient_uniqu': ['000000001', '000000002', '000000003']
    })

    expected_df = pd.DataFrame.from_dict({
        'awardee_or_recipient_uniqu': ['000000001', '000000002', '000000003'],
        'address_line_1': ['Test address 1', 'Other Test address 1', None],
        'address_line_2': ['Test address 2', 'Other Test address 2', None],
        'city': ['Test city', 'Other Test city', None],
        'state': ['Test state', 'Other Test state', None],
        'zip': ['Test zip', 'Other Test zip', None],
        'zip4': ['Test zip4', 'Other Test zip4', None],
        'country_code': ['Test country', 'Other Test country', None],
        'congressional_district': ['Test congressional district', 'Other Test congressional district', None],
        'business_types_codes': [['A', 'B', 'C'], ['D', 'E', 'F'], []],
        'dba_name': ['Name 1', 'Name 2', None],
        'ultimate_parent_unique_ide': ['999999999', '999999998', None],
        'ultimate_parent_legal_enti': ['Parent Legal Name 1', 'Parent Legal Name 2', None]
    })

    assert expected_df.sort_index(inplace=True) == update_historical_duns.update_duns_props(duns_df, None)\
        .sort_index(inplace=True)


def test_update_duns_props_empty(monkeypatch):
    """ Special case where no data is returned """
    monkeypatch.setattr('dataactcore.utils.parentDuns.get_duns_props_from_sam',
                        mock_get_duns_props_from_sam)
    duns_df = pd.DataFrame.from_dict({
            'awardee_or_recipient_uniqu': ['000000003']
        })

    expected_df = pd.DataFrame.from_dict({
        'awardee_or_recipient_uniqu': ['000000003'],
        'address_line_1': [None],
        'address_line_2': [None],
        'city': [None],
        'state': [None],
        'zip': [None],
        'zip4': [None],
        'country_code': [None],
        'congressional_district': [None],
        'business_types_codes': [[]],
        'dba_name': [None],
        'ultimate_parent_unique_ide': [None],
        'ultimate_parent_legal_enti': [None]
    })

    assert expected_df.to_dict() == update_historical_duns.update_duns_props(duns_df, None).to_dict()


def test_run_duns_batches(database, monkeypatch):
    """ Test run_duns_batches for the core functionality """
    monkeypatch.setattr('dataactcore.utils.parentDuns.get_duns_props_from_sam',
                        mock_get_duns_props_from_sam)
    sess = database.session
    all_duns = ['00000000{}'.format(x) for x in range(1, 5)]
    existing_duns = all_duns[2:]
    for duns in existing_duns:
        sess.add(DUNS(awardee_or_recipient_uniqu=duns))
    sess.commit()

    duns_file = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'historic_DUNS_export_small.csv')

    update_historical_duns.run_duns_batches(duns_file, sess, None, block_size=1)

    expected_results = {
        '000000001': {
            'awardee_or_recipient_uniqu': '000000001',
            'registration_date': '2004-04-01',
            'expiration_date': '2013-01-11',
            'last_sam_mod_date': '2013-01-11',
            'activation_date': '2012-01-11',
            'legal_business_name': 'TEST DUNS 1',
            'address_line_1': 'Test address 1',
            'address_line_2': 'Test address 2',
            'city': 'Test city',
            'state': 'Test state',
            'zip': 'Test zip',
            'zip4': 'Test zip4',
            'country_code': 'Test country',
            'congressional_district': 'Test congressional district',
            'business_types_codes': ['A', 'B', 'C'],
            'dba_name': 'Name 1',
            'ultimate_parent_unique_ide': '999999999',
            'ultimate_parent_legal_enti': 'Parent Legal Name 1'
        },
        '000000002': {
            'awardee_or_recipient_uniqu': '000000002',
            'registration_date': '2004-04-02',
            'expiration_date': '2013-01-12',
            'last_sam_mod_date': '2013-01-12',
            'activation_date': '2012-01-12',
            'legal_business_name': 'TEST DUNS 2',
            'address_line_1': 'Other Test address 1',
            'address_line_2': 'Other Test address 2',
            'city': 'Other Test city',
            'state': 'Other Test state',
            'zip': 'Other Test zip',
            'zip4': 'Other Test zip4',
            'country_code': 'Other Test country',
            'congressional_district': 'Other Test congressional district',
            'business_types_codes': ['D', 'E', 'F'],
            'dba_name': 'Name 2',
            'ultimate_parent_unique_ide': '999999998',
            'ultimate_parent_legal_enti': 'Parent Legal Name 2'
        }
    }
    results = {}
    for duns_obj in sess.query(HistoricDUNS).all():
        results[duns_obj.awardee_or_recipient_uniqu] = {
            'awardee_or_recipient_uniqu': duns_obj.awardee_or_recipient_uniqu,
            'registration_date': str(duns_obj.registration_date) if duns_obj.registration_date else None,
            'expiration_date': str(duns_obj.expiration_date) if duns_obj.expiration_date else None,
            'last_sam_mod_date': str(duns_obj.last_sam_mod_date) if duns_obj.last_sam_mod_date else None,
            'activation_date': str(duns_obj.activation_date) if duns_obj.activation_date else None,
            'legal_business_name': duns_obj.legal_business_name,
            'address_line_1': duns_obj.address_line_1,
            'address_line_2': duns_obj.address_line_2,
            'city': duns_obj.city,
            'state': duns_obj.state,
            'zip': duns_obj.zip,
            'zip4': duns_obj.zip4,
            'country_code': duns_obj.country_code,
            'congressional_district': duns_obj.congressional_district,
            'business_types_codes': duns_obj.business_types_codes,
            'dba_name': duns_obj.dba_name,
            'ultimate_parent_unique_ide': duns_obj.ultimate_parent_unique_ide,
            'ultimate_parent_legal_enti': duns_obj.ultimate_parent_legal_enti
        }
    assert results == expected_results


def test_workflows(database, monkeypatch):
    """ Test both scenarios of the script, starting with a full run """
    monkeypatch.setattr('dataactcore.utils.parentDuns.get_duns_props_from_sam',
                        mock_get_duns_props_from_sam)
    sess = database.session
    all_duns = ['00000000{}'.format(x) for x in range(1, 5)]
    existing_duns = all_duns[2:]
    for duns in existing_duns:
        sess.add(DUNS(awardee_or_recipient_uniqu=duns))
    sess.commit()

    duns_file = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'historic_DUNS_export_small.csv')

    update_historical_duns.run_duns_batches(duns_file, sess, None, block_size=1)
    update_historical_duns.import_historic_duns(sess)

    expected_results = {
        '000000001': {
            'awardee_or_recipient_uniqu': '000000001',
            'registration_date': '2004-04-01',
            'expiration_date': '2013-01-11',
            'last_sam_mod_date': '2013-01-11',
            'activation_date': '2012-01-11',
            'legal_business_name': 'TEST DUNS 1',
            'address_line_1': 'Test address 1',
            'address_line_2': 'Test address 2',
            'city': 'Test city',
            'state': 'Test state',
            'zip': 'Test zip',
            'zip4': 'Test zip4',
            'country_code': 'Test country',
            'congressional_district': 'Test congressional district',
            'business_types_codes': ['A', 'B', 'C'],
            'dba_name': 'Name 1',
            'ultimate_parent_unique_ide': '999999999',
            'ultimate_parent_legal_enti': 'Parent Legal Name 1'
        },
        '000000002': {
            'awardee_or_recipient_uniqu': '000000002',
            'registration_date': '2004-04-02',
            'expiration_date': '2013-01-12',
            'last_sam_mod_date': '2013-01-12',
            'activation_date': '2012-01-12',
            'legal_business_name': 'TEST DUNS 2',
            'address_line_1': 'Other Test address 1',
            'address_line_2': 'Other Test address 2',
            'city': 'Other Test city',
            'state': 'Other Test state',
            'zip': 'Other Test zip',
            'zip4': 'Other Test zip4',
            'country_code': 'Other Test country',
            'congressional_district': 'Other Test congressional district',
            'business_types_codes': ['D', 'E', 'F'],
            'dba_name': 'Name 2',
            'ultimate_parent_unique_ide': '999999998',
            'ultimate_parent_legal_enti': 'Parent Legal Name 2'
        },
        '000000003': {
            'awardee_or_recipient_uniqu': '000000003',
            'registration_date': None,
            'expiration_date': None,
            'last_sam_mod_date': None,
            'activation_date': None,
            'legal_business_name': None,
            'address_line_1': None,
            'address_line_2': None,
            'city': None,
            'state': None,
            'zip': None,
            'zip4': None,
            'country_code': None,
            'congressional_district': None,
            'business_types_codes': None,
            'dba_name': None,
            'ultimate_parent_unique_ide': None,
            'ultimate_parent_legal_enti': None
        },
        '000000004': {
            'awardee_or_recipient_uniqu': '000000004',
            'registration_date': None,
            'expiration_date': None,
            'last_sam_mod_date': None,
            'activation_date': None,
            'legal_business_name': None,
            'address_line_1': None,
            'address_line_2': None,
            'city': None,
            'state': None,
            'zip': None,
            'zip4': None,
            'country_code': None,
            'congressional_district': None,
            'business_types_codes': None,
            'dba_name': None,
            'ultimate_parent_unique_ide': None,
            'ultimate_parent_legal_enti': None
        }
    }
    results = {}
    for duns_obj in sess.query(DUNS).all():
        results[duns_obj.awardee_or_recipient_uniqu] = {
            'awardee_or_recipient_uniqu': duns_obj.awardee_or_recipient_uniqu,
            'registration_date': str(duns_obj.registration_date) if duns_obj.registration_date else None,
            'expiration_date': str(duns_obj.expiration_date) if duns_obj.expiration_date else None,
            'last_sam_mod_date': str(duns_obj.last_sam_mod_date) if duns_obj.last_sam_mod_date else None,
            'activation_date': str(duns_obj.activation_date) if duns_obj.activation_date else None,
            'legal_business_name': duns_obj.legal_business_name,
            'address_line_1': duns_obj.address_line_1,
            'address_line_2': duns_obj.address_line_2,
            'city': duns_obj.city,
            'state': duns_obj.state,
            'zip': duns_obj.zip,
            'zip4': duns_obj.zip4,
            'country_code': duns_obj.country_code,
            'congressional_district': duns_obj.congressional_district,
            'business_types_codes': duns_obj.business_types_codes,
            'dba_name': duns_obj.dba_name,
            'ultimate_parent_unique_ide': duns_obj.ultimate_parent_unique_ide,
            'ultimate_parent_legal_enti': duns_obj.ultimate_parent_legal_enti
        }
    assert results == expected_results

    # Test to see if truncating the DUNS table while keeping the historic reuploads the historic values
    sess.query(DUNS).filter(DUNS.historic.is_(True)).delete(synchronize_session=False)

    # Make sure all the historic DUNS are removed from the DUNS table
    assert sess.query(DUNS).filter(DUNS.historic.is_(True)).all() == []

    # Redo script but don't go through run_duns_batches
    update_historical_duns.clean_historic_duns(sess)
    update_historical_duns.import_historic_duns(sess)

    results = {}
    for duns_obj in sess.query(DUNS).all():
        results[duns_obj.awardee_or_recipient_uniqu] = {
            'awardee_or_recipient_uniqu': duns_obj.awardee_or_recipient_uniqu,
            'registration_date': str(duns_obj.registration_date) if duns_obj.registration_date else None,
            'expiration_date': str(duns_obj.expiration_date) if duns_obj.expiration_date else None,
            'last_sam_mod_date': str(duns_obj.last_sam_mod_date) if duns_obj.last_sam_mod_date else None,
            'activation_date': str(duns_obj.activation_date) if duns_obj.activation_date else None,
            'legal_business_name': duns_obj.legal_business_name,
            'address_line_1': duns_obj.address_line_1,
            'address_line_2': duns_obj.address_line_2,
            'city': duns_obj.city,
            'state': duns_obj.state,
            'zip': duns_obj.zip,
            'zip4': duns_obj.zip4,
            'country_code': duns_obj.country_code,
            'congressional_district': duns_obj.congressional_district,
            'business_types_codes': duns_obj.business_types_codes,
            'dba_name': duns_obj.dba_name,
            'ultimate_parent_unique_ide': duns_obj.ultimate_parent_unique_ide,
            'ultimate_parent_legal_enti': duns_obj.ultimate_parent_legal_enti
        }
    assert results == expected_results


def test_clean_historic_duns(database, monkeypatch):
    """
        Test to make sure if a new DUNS is loaded and we reload historic DUNS (skipping the major load),
        we should remove the historic equivalents.
    """
    monkeypatch.setattr('dataactcore.utils.parentDuns.get_duns_props_from_sam',
                        mock_get_duns_props_from_sam)
    sess = database.session
    all_duns = ['00000000{}'.format(x) for x in range(1, 5)]
    existing_duns = all_duns[2:]
    for duns in existing_duns:
        sess.add(DUNS(awardee_or_recipient_uniqu=duns))
    sess.commit()

    duns_file = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'historic_DUNS_export_small.csv')

    # normal run
    update_historical_duns.run_duns_batches(duns_file, sess, None, block_size=1)
    update_historical_duns.import_historic_duns(sess)

    # update old DUNS as part of load_duns.py
    updated_duns = sess.query(DUNS).filter(DUNS.awardee_or_recipient_uniqu == '000000002').one()
    updated_duns.historic = False
    sess.commit()

    # rerun with a skip
    update_historical_duns.clean_historic_duns(sess)
    update_historical_duns.import_historic_duns(sess)

    # check to see if historic duns equivalent is removed
    expected_count = sess.query(HistoricDUNS).filter(HistoricDUNS.awardee_or_recipient_uniqu == '000000002').count()
    assert expected_count == 0
