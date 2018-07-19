import pandas as pd
import os

from dataactcore.config import CONFIG_BROKER
from dataactbroker.scripts import update_historical_duns
from dataactcore.models.domainModels import DUNS


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


def test_clean_duns_csv_data():
    dirty_data = {
        'awardee_or_recipient_uniqu': [None, '000000000'],
        'registration_date': [None, '2007-10-01'],
        'expiration_date': [None, '2007-10-03'],
        'last_sam_mod_date': [None, '2007-10-03'],
        'activation_date': [None, '2007-10-04'],
        'legal_business_name': [None, 'Test name'],
        'address_line_1': [None, 'Test address 1'],
        'address_line_2': [None, 'Test address 2'],
        'city': [None, 'Test city'],
        'state': [None, 'Test state'],
        'zip': [None, 'Test zip'],
        'zip4': [None, 'Test zip4'],
        'country_code': [None, 'Test country'],
        'congressional_district': [None, 'Test congressional district'],
        'business_types_codes': [None, ['A', 'B', 'C']]
    }
    # to_dict() returns the index followed by the duns
    clean_data = {
        'awardee_or_recipient_uniqu': {1: '000000000'},
        'registration_date': {1: '2007-10-01'},
        'expiration_date': {1: '2007-10-03'},
        'last_sam_mod_date': {1: '2007-10-03'},
        'activation_date': {1: '2007-10-04'},
        'legal_business_name': {1: 'Test name'},
        'address_line_1': {1: 'Test address 1'},
        'address_line_2': {1: 'Test address 2'},
        'city': {1: 'Test city'},
        'state': {1: 'Test state'},
        'zip': {1: 'Test zip'},
        'zip4': {1: 'Test zip4'},
        'country_code': {1: 'Test country'},
        'congressional_district': {1: 'Test congressional district'},
        'business_types_codes': {1: ['A', 'B', 'C']}
    }
    data = pd.DataFrame.from_dict(dirty_data)
    cleaned_data = update_historical_duns.clean_duns_csv_data(data).to_dict()
    # can't predict what the created_at, updated_at, can just test whether it has those columns
    assert 'created_at' in cleaned_data
    del cleaned_data['created_at']
    assert 'updated_at' in cleaned_data
    del cleaned_data['updated_at']
    assert clean_data == cleaned_data


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


def mock_get_location_business_from_sam(client, duns_list):
    """ Mock function for location_business data as we can't connect to the SAM service """
    columns = ['awardee_or_recipient_uniqu'] + list(update_historical_duns.props_columns.keys())
    results = pd.DataFrame(columns=columns)
    duns_mappings = {
        '000000001': {
            'awardee_or_recipient_uniqu': ['000000001'],
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
    """ Testing updating the duns props with both populated/blank location/business data """
    monkeypatch.setattr('dataactcore.utils.parentDuns.get_location_business_from_sam',
                        mock_get_location_business_from_sam)
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
        'business_types_codes': [['A', 'B', 'C'], ['D', 'E', 'F'], []]
    })

    assert expected_df.sort_index(inplace=True) == update_historical_duns.update_duns_props(duns_df, None)\
        .sort_index(inplace=True)


def test_update_duns_props_empty(monkeypatch):
    """ Special case where no location/business data is returned """
    monkeypatch.setattr('dataactcore.utils.parentDuns.get_location_business_from_sam',
                        mock_get_location_business_from_sam)
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
        'business_types_codes': [[]]
    })

    assert expected_df.to_dict() == update_historical_duns.update_duns_props(duns_df, None).to_dict()


def test_run_duns_batches(database, monkeypatch):
    """ Overall test of the update_historical_duns script testing most of the functionality """
    monkeypatch.setattr('dataactcore.utils.parentDuns.get_location_business_from_sam',
                        mock_get_location_business_from_sam)
    sess = database.session
    all_duns = ['00000000{}'.format(x) for x in range(1, 5)]
    existing_duns = all_duns[2:]
    for duns in existing_duns:
        sess.add(DUNS(awardee_or_recipient_uniqu=duns))
    sess.commit()

    duns_file = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'DUNS_export_small.csv')

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
            'business_types_codes': ['A', 'B', 'C']
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
            'business_types_codes': ['D', 'E', 'F']
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
            'business_types_codes': None
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
            'business_types_codes': None
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
            'business_types_codes': duns_obj.business_types_codes
        }
    assert results == expected_results
