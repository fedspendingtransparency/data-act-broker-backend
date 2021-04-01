import os
import datetime
import pandas as pd

from dataactcore.config import CONFIG_BROKER
from dataactcore.scripts import load_duns_exec_comp
from dataactcore.models.domainModels import DUNS
from dataactcore.utils.duns import DUNS_COLUMNS, EXCLUDE_FROM_API


def mock_get_duns_props_from_sam(duns_list):
    """ Mock function for get_duns_props as we can't connect to the SAM service """
    request_cols = [col for col in DUNS_COLUMNS if col not in EXCLUDE_FROM_API]
    columns = request_cols
    results = pd.DataFrame(columns=columns)

    duns_mappings = {
        '000000001': {
            'awardee_or_recipient_uniqu': '000000001',
            'uei': 'A1',
            'legal_business_name': 'Legal Name 1',
            'dba_name': 'Name 1',
            'entity_structure': '1A',
            'ultimate_parent_unique_ide': '000000004',
            'ultimate_parent_uei': 'D4',
            'ultimate_parent_legal_enti': 'Parent Legal Name 1',
            'address_line_1': 'Test address 1',
            'address_line_2': 'Test address 2',
            'city': 'Test city',
            'state': 'Test state',
            'zip': 'Test zip',
            'zip4': 'Test zip4',
            'country_code': 'Test country',
            'congressional_district': 'Test congressional district',
            'business_types_codes': [['A', 'B', 'C']],
            'business_types': [['Name A', 'Name B', 'Name C']],
            'high_comp_officer1_full_na': 'Test Exec 1',
            'high_comp_officer1_amount': '1',
            'high_comp_officer2_full_na': 'Test Exec 2',
            'high_comp_officer2_amount': '2',
            'high_comp_officer3_full_na': 'Test Exec 3',
            'high_comp_officer3_amount': '3',
            'high_comp_officer4_full_na': 'Test Exec 4',
            'high_comp_officer4_amount': '4',
            'high_comp_officer5_full_na': 'Test Exec 5',
            'high_comp_officer5_amount': '5'
        },
        '000000005': {
            'awardee_or_recipient_uniqu': '000000005',
            'uei': 'E5',
            'legal_business_name': 'Legal Name 2',
            'dba_name': 'Name 2',
            'entity_structure': '2B',
            'ultimate_parent_unique_ide': None,
            'ultimate_parent_uei': None,
            'ultimate_parent_legal_enti': None,
            'address_line_1': 'Other Test address 1',
            'address_line_2': 'Other Test address 2',
            'city': 'Other Test city',
            'state': 'Other Test state',
            'zip': 'Other Test zip',
            'zip4': 'Other Test zip4',
            'country_code': 'Other Test country',
            'congressional_district': 'Other Test congressional district',
            'business_types_codes': [['D', 'E', 'F']],
            'business_types': [['Name D', 'Name E', 'Name F']],
            'high_comp_officer1_full_na': 'Test Other Exec 6',
            'high_comp_officer1_amount': '6',
            'high_comp_officer2_full_na': 'Test Other Exec 7',
            'high_comp_officer2_amount': '7',
            'high_comp_officer3_full_na': 'Test Other Exec 8',
            'high_comp_officer3_amount': '8',
            'high_comp_officer4_full_na': 'Test Other Exec 9',
            'high_comp_officer4_amount': '9',
            'high_comp_officer5_full_na': 'Test Other Exec 10',
            'high_comp_officer5_amount': '10'
        }
    }
    for duns in duns_list:
        if duns in duns_mappings:
            results = results.append(pd.DataFrame(dict([(k, pd.Series(v)) for k, v in duns_mappings[duns].items()])),
                                     sort=True)
    return results


def test_load_duns(database, monkeypatch):
    """ Test a local run load duns with the test files """
    monkeypatch.setattr('dataactcore.utils.duns.get_duns_props_from_sam', mock_get_duns_props_from_sam)
    sess = database.session
    duns_dir = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'fake_sam_files', 'duns')

    load_duns_exec_comp.load_from_sam('DUNS', sess, True, duns_dir)

    # update if the fake DUNS file name/zip changes
    deactivation_date = '2021-02-06'

    expected_results = {
        # Pulled active monthly record, slightly updated with deactivation date as sam_extract = 1
        '000000001': {
            'uei': 'A1',
            'awardee_or_recipient_uniqu': '000000001',
            'registration_date': '1999-01-01',
            'activation_date': '1999-01-02',
            'expiration_date': '1999-01-25',
            'last_sam_mod_date': '1999-01-15',
            'deactivation_date': deactivation_date,
            'legal_business_name': 'LEGAL BUSINESS NAME 000000001 V1 MONTHLY',
            'address_line_1': 'ADDRESS LINE 1 000000001 V1 MONTHLY',
            'address_line_2': 'ADDRESS LINE 2 000000001 V1 MONTHLY',
            'city': 'CITY 000000001 V1 MONTHLY',
            'state': 'ST 000000001 V1 MONTHLY',
            'zip': 'ZIP 000000001 V1 MONTHLY',
            'zip4': 'ZIP4 000000001 V1 MONTHLY',
            'country_code': 'COUNTRY 000000001 V1 MONTHLY',
            'congressional_district': 'CONGRESSIONAL DISTRICT 000000001 V1 MONTHLY',
            'business_types_codes': ['2X', 'MF'],
            'business_types': ['For Profit Organization', 'Manufacturer of Goods'],
            'dba_name': 'DBA NAME 000000001 V1 MONTHLY',
            'ultimate_parent_uei': 'D4',
            'ultimate_parent_unique_ide': '000000004',
            'ultimate_parent_legal_enti': 'ULTIMATE PARENT LEGAL BUSINESS NAME 000000004 V1 MONTHLY',
            'historic': False
        },
        # Pulled active monthly record, updated as sam_extract = 2, don't pull in dup delete record
        '000000002': {
            'uei': 'B2',
            'awardee_or_recipient_uniqu': '000000002',
            'registration_date': '2000-02-01',
            'activation_date': '2000-02-02',
            'expiration_date': '2000-02-25',
            'last_sam_mod_date': '2000-02-15',
            'deactivation_date': None,
            'legal_business_name': 'LEGAL BUSINESS NAME B2 V2 DAILY',
            'address_line_1': 'ADDRESS LINE 1 B2 V2 DAILY',
            'address_line_2': 'ADDRESS LINE 2 B2 V2 DAILY',
            'city': 'CITY B2 V2 DAILY',
            'state': 'ST B2 V2 DAILY',
            'zip': 'ZIP B2 V2 DAILY',
            'zip4': 'ZIP4 B2 V2 DAILY',
            'country_code': 'COUNTRY B2 V2 DAILY',
            'congressional_district': 'CONGRESSIONAL DISTRICT B2 V2 DAILY',
            'business_types_codes': ['2X', 'MF'],
            'business_types': ['For Profit Organization', 'Manufacturer of Goods'],
            'dba_name': 'DBA NAME B2 V2 DAILY',
            'ultimate_parent_uei': 'E5',
            'ultimate_parent_unique_ide': '000000005',
            'ultimate_parent_legal_enti': 'ULTIMATE PARENT LEGAL BUSINESS NAME E5 V2 DAILY',
            'historic': False
        },
        # Pulled active monthly record, updated as sam_extract = 3
        '000000003': {
            'uei': 'C3',
            'awardee_or_recipient_uniqu': '000000003',
            'registration_date': '2000-03-01',
            'activation_date': '2000-03-02',
            'expiration_date': '2000-03-25',
            'last_sam_mod_date': '2000-03-15',
            'deactivation_date': None,
            'legal_business_name': 'LEGAL BUSINESS NAME C3 V2 DAILY',
            'address_line_1': 'ADDRESS LINE 1 C3 V2 DAILY',
            'address_line_2': 'ADDRESS LINE 2 C3 V2 DAILY',
            'city': 'CITY C3 V2 DAILY',
            'state': 'ST C3 V2 DAILY',
            'zip': 'ZIP C3 V2 DAILY',
            'zip4': 'ZIP4 C3 V2 DAILY',
            'country_code': 'COUNTRY C3 V2 DAILY',
            'congressional_district': 'CONGRESSIONAL DISTRICT C3 V2 DAILY',
            'business_types_codes': ['2X', 'MF'],
            'business_types': ['For Profit Organization', 'Manufacturer of Goods'],
            'dba_name': 'DBA NAME C3 V2 DAILY',
            'ultimate_parent_uei': 'F6',
            'ultimate_parent_unique_ide': '000000006',
            'ultimate_parent_legal_enti': 'ULTIMATE PARENT LEGAL BUSINESS NAME F6 V2 DAILY',
            'historic': False
        },
        # Pulled active daily V1 record, updated in daily V2 record
        '000000004': {
            'uei': 'D4',
            'awardee_or_recipient_uniqu': '000000004',
            'registration_date': '2000-03-01',
            'activation_date': '2000-03-02',
            'expiration_date': '2000-03-25',
            'last_sam_mod_date': '2000-03-15',
            'deactivation_date': None,
            'legal_business_name': 'LEGAL BUSINESS NAME D4 V2 DAILY',
            'address_line_1': 'ADDRESS LINE 1 D4 V2 DAILY',
            'address_line_2': 'ADDRESS LINE 2 D4 V2 DAILY',
            'city': 'CITY D4 V2 DAILY',
            'state': 'ST D4 V2 DAILY',
            'zip': 'ZIP D4 V2 DAILY',
            'zip4': 'ZIP4 D4 V2 DAILY',
            'country_code': 'COUNTRY D4 V2 DAILY',
            'congressional_district': 'CONGRESSIONAL DISTRICT D4 V2 DAILY',
            'business_types_codes': ['2X', 'MF'],
            'business_types': ['For Profit Organization', 'Manufacturer of Goods'],
            'dba_name': 'DBA NAME D4 V2 DAILY',
            'ultimate_parent_uei': 'G7',
            'ultimate_parent_unique_ide': '000000007',
            'ultimate_parent_legal_enti': 'ULTIMATE PARENT LEGAL BUSINESS NAME G7 V2 DAILY',
            'historic': False
        },
        # Pulled active daily V1 record, not updated in V2
        '000000005': {
            'uei': 'E5',
            'awardee_or_recipient_uniqu': '000000005',
            'registration_date': '2000-02-01',
            'activation_date': '2000-02-02',
            'expiration_date': '2000-02-25',
            'last_sam_mod_date': '2000-02-15',
            'deactivation_date': None,
            'legal_business_name': 'LEGAL BUSINESS NAME 000000005 V1 DAILY',
            'address_line_1': 'ADDRESS LINE 1 000000005 V1 DAILY',
            'address_line_2': 'ADDRESS LINE 2 000000005 V1 DAILY',
            'city': 'CITY 000000005 V1 DAILY',
            'state': 'ST 000000005 V1 DAILY',
            'zip': 'ZIP 000000005 V1 DAILY',
            'zip4': 'ZIP4 000000005 V1 DAILY',
            'country_code': 'COUNTRY 000000005 V1 DAILY',
            'congressional_district': 'CONGRESSIONAL DISTRICT 000000005 V1 DAILY',
            'business_types_codes': ['2X', 'MF'],
            'business_types': ['For Profit Organization', 'Manufacturer of Goods'],
            'dba_name': 'DBA NAME 000000005 V1 DAILY',
            'ultimate_parent_uei': None,
            'ultimate_parent_unique_ide': '000000007',
            'ultimate_parent_legal_enti': 'ULTIMATE PARENT LEGAL BUSINESS NAME G7 V2 DAILY',  # via missing parent names
            'historic': False
        }
    }

    # Ensure duplicates are covered
    expected_duns_count = 5
    duns_count = sess.query(DUNS).count()
    assert duns_count == expected_duns_count

    results = {}
    for duns_obj in sess.query(DUNS).all():
        results[duns_obj.awardee_or_recipient_uniqu] = {
            'uei': duns_obj.uei,
            'awardee_or_recipient_uniqu': duns_obj.awardee_or_recipient_uniqu,
            'registration_date': str(duns_obj.registration_date) if duns_obj.registration_date else None,
            'activation_date': str(duns_obj.activation_date) if duns_obj.activation_date else None,
            'expiration_date': str(duns_obj.expiration_date) if duns_obj.expiration_date else None,
            'last_sam_mod_date': str(duns_obj.last_sam_mod_date) if duns_obj.last_sam_mod_date else None,
            'deactivation_date': str(duns_obj.deactivation_date) if duns_obj.deactivation_date else None,
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
            'business_types': duns_obj.business_types,
            'dba_name': duns_obj.dba_name,
            'ultimate_parent_uei': duns_obj.ultimate_parent_uei,
            'ultimate_parent_unique_ide': duns_obj.ultimate_parent_unique_ide,
            'ultimate_parent_legal_enti': duns_obj.ultimate_parent_legal_enti,
            'historic': duns_obj.historic
        }
    assert results == expected_results


def test_load_exec_comp(database, monkeypatch):
    """ Test a local run load exec_comp with the test files """
    monkeypatch.setattr('dataactcore.utils.duns.get_duns_props_from_sam', mock_get_duns_props_from_sam)
    sess = database.session
    duns_dir = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'fake_sam_files', 'duns')
    exec_comp_dir = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'fake_sam_files', 'exec_comp')

    load_duns_exec_comp.load_from_sam('DUNS', sess, True, duns_dir)
    load_duns_exec_comp.load_from_sam('Executive Compensation', sess, True, exec_comp_dir, None)

    monthly_last_exec_date = datetime.date(2017, 9, 30)
    daily_last_exec_date = datetime.date(2019, 3, 29)

    expected_results = {
        # processed the earliest in the monthly, not updated as sam_extract = 1
        '000000001': {
            'awardee_or_recipient_uniqu': '000000001',
            'high_comp_officer1_full_na': 'Terence Test 1',
            'high_comp_officer1_amount': '11952013',
            'high_comp_officer2_full_na': 'Aaron Test 1',
            'high_comp_officer2_amount': '41161',
            'high_comp_officer3_full_na': 'Jason Test 1',
            'high_comp_officer3_amount': '286963',
            'high_comp_officer4_full_na': 'Michael Test 1',
            'high_comp_officer4_amount': '129337',
            'high_comp_officer5_full_na': 'Mark Test 1',
            'high_comp_officer5_amount': '1248877',
            'last_exec_comp_mod_date': monthly_last_exec_date
        },
        # processed the earliest in the monthly, processed latest of daily as sam_extract = 2
        '000000002': {
            'awardee_or_recipient_uniqu': '000000002',
            'high_comp_officer1_full_na': 'Terence Test Updated 2',
            'high_comp_officer1_amount': '21952013',
            'high_comp_officer2_full_na': 'Aaron Test Updated 2',
            'high_comp_officer2_amount': '51161',
            'high_comp_officer3_full_na': 'Jason Test Updated 2',
            'high_comp_officer3_amount': '386963',
            'high_comp_officer4_full_na': 'Michael Test Updated 2',
            'high_comp_officer4_amount': '329337',
            'high_comp_officer5_full_na': 'Mark Test Updated 2',
            'high_comp_officer5_amount': '3248877',
            'last_exec_comp_mod_date': daily_last_exec_date
        },
        # processed the only one in the monthly, processed only one in daily as sam_extract = 3
        '000000003': {
            'awardee_or_recipient_uniqu': '000000003',
            'high_comp_officer1_full_na': 'Terence Test Updated 1',
            'high_comp_officer1_amount': '21952013',
            'high_comp_officer2_full_na': 'Aaron Test Updated 1',
            'high_comp_officer2_amount': '51161',
            'high_comp_officer3_full_na': 'Jason Test Updated 1',
            'high_comp_officer3_amount': '386963',
            'high_comp_officer4_full_na': 'Michael Test Updated 1',
            'high_comp_officer4_amount': '329337',
            'high_comp_officer5_full_na': 'Mark Test Updated 1',
            'high_comp_officer5_amount': '3248877',
            'last_exec_comp_mod_date': daily_last_exec_date
        },
        '000000004': {
            'awardee_or_recipient_uniqu': '000000004',
            'high_comp_officer1_full_na': None,
            'high_comp_officer1_amount': None,
            'high_comp_officer2_full_na': None,
            'high_comp_officer2_amount': None,
            'high_comp_officer3_full_na': None,
            'high_comp_officer3_amount': None,
            'high_comp_officer4_full_na': None,
            'high_comp_officer4_amount': None,
            'high_comp_officer5_full_na': None,
            'high_comp_officer5_amount': None,
            'last_exec_comp_mod_date': None
        },
        '000000005': {
            'awardee_or_recipient_uniqu': '000000005',
            'high_comp_officer1_full_na': None,
            'high_comp_officer1_amount': None,
            'high_comp_officer2_full_na': None,
            'high_comp_officer2_amount': None,
            'high_comp_officer3_full_na': None,
            'high_comp_officer3_amount': None,
            'high_comp_officer4_full_na': None,
            'high_comp_officer4_amount': None,
            'high_comp_officer5_full_na': None,
            'high_comp_officer5_amount': None,
            'last_exec_comp_mod_date': None
        }
    }
    results = {}
    for duns_obj in sess.query(DUNS).all():
        results[duns_obj.awardee_or_recipient_uniqu] = {
            'awardee_or_recipient_uniqu': duns_obj.awardee_or_recipient_uniqu,
            'high_comp_officer1_full_na': duns_obj.high_comp_officer1_full_na,
            'high_comp_officer1_amount': duns_obj.high_comp_officer1_amount,
            'high_comp_officer2_full_na': duns_obj.high_comp_officer2_full_na,
            'high_comp_officer2_amount': duns_obj.high_comp_officer2_amount,
            'high_comp_officer3_full_na': duns_obj.high_comp_officer3_full_na,
            'high_comp_officer3_amount': duns_obj.high_comp_officer3_amount,
            'high_comp_officer4_full_na': duns_obj.high_comp_officer4_full_na,
            'high_comp_officer4_amount': duns_obj.high_comp_officer4_amount,
            'high_comp_officer5_full_na': duns_obj.high_comp_officer5_full_na,
            'high_comp_officer5_amount': duns_obj.high_comp_officer5_amount,
            'last_exec_comp_mod_date': duns_obj.last_exec_comp_mod_date
        }
    assert results == expected_results
