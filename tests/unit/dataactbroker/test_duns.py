import os
import datetime

from dataactcore.config import CONFIG_BROKER
from dataactcore.scripts import load_duns, load_exec_comp
from dataactcore.models.domainModels import DUNS


def test_load_duns(database):
    """ Test a local run load duns with the test files """
    sess = database.session
    duns_dir = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'fake_sam_files', 'duns')

    load_duns.process_duns_dir(sess, True, duns_dir)

    expected_results = {
        # Updated with deactivation date as sam_extract = 1
        '000000000': {
            'awardee_or_recipient_uniqu': '000000000',
            'registration_date': '2003-09-18',
            'expiration_date': '2015-08-25',
            'last_sam_mod_date': '2014-08-21',
            'activation_date': '2014-08-21',
            'deactivation_date': '2019-07-20',
            'legal_business_name': 'TEST ADMINISTRATION',
            'address_line_1': '1205 NEW JERSEY AVE SS',
            'address_line_2': None,
            'city': 'WASHINGTON',
            'state': 'DC',
            'zip': '20690',
            'zip4': '0002',
            'country_code': 'USA',
            'congressional_district': '99',
            'business_types_codes': ['2R', 'NG', '32'],
            'dba_name': None,
            'ultimate_parent_unique_ide': '161906193',
            'ultimate_parent_legal_enti': None,
            'historic': False
        },
        # Updated as sam_extract = 2
        '000000001': {
            'awardee_or_recipient_uniqu': '000000001',
            'registration_date': '2005-08-26',
            'expiration_date': '2015-07-08',
            'last_sam_mod_date': '2014-07-08',
            'activation_date': '2014-07-08',
            'deactivation_date': None,
            'legal_business_name': 'TEST SERVICE, UNITED STATES UPDATED',
            'address_line_1': '2609 JEFFERSON TEST PARKWAY UPDATED',
            'address_line_2': None,
            'city': 'ALEXANDRIA',
            'state': 'VA',
            'zip': '22301',
            'zip4': '1025',
            'country_code': 'USA',
            'congressional_district': '08',
            'business_types_codes': ['2R'],
            'dba_name': None,
            'ultimate_parent_unique_ide': '161906193',
            'ultimate_parent_legal_enti': None,
            'historic': False
        },
        # Updated as sam_extract = 3
        '000000002': {
            'awardee_or_recipient_uniqu': '000000002',
            'registration_date': '2004-08-13',
            'expiration_date': '2015-10-02',
            'last_sam_mod_date': '2014-10-02',
            'activation_date': '2014-10-02',
            'deactivation_date': None,
            'legal_business_name': 'NATIONAL TEST ADMINISTRATION UPDATED',
            'address_line_1': '309 E AVE SS UPDATED',
            'address_line_2': None,
            'city': 'WASHINGTON',
            'state': 'DC',
            'zip': '20546',
            'zip4': '0002',
            'country_code': 'USA',
            'congressional_district': '98',
            'business_types_codes': ['2R'],
            'dba_name': 'NTA',
            'ultimate_parent_unique_ide': '161906193',
            'ultimate_parent_legal_enti': None,
            'historic': False
        },
        # Not updated as sam_extract = 4
        '000000003': {
            'awardee_or_recipient_uniqu': '000000003',
            'registration_date': '2004-09-03',
            'expiration_date': '2015-10-09',
            'last_sam_mod_date': '2014-10-09',
            'activation_date': '2014-10-09',
            'deactivation_date': None,
            'legal_business_name': 'NATIONAL PRAISE FOR THE TEST',
            'address_line_1': '409 THIRTEENTH ST SW',
            'address_line_2': None,
            'city': 'WASHINGTON',
            'state': 'DC',
            'zip': '20506',
            'zip4': '0001',
            'country_code': 'USA',
            'congressional_district': '98',
            'business_types_codes': ['2R'],
            'dba_name': None,
            'ultimate_parent_unique_ide': '161906193',
            'ultimate_parent_legal_enti': None,
            'historic': False
        },
        # Not updated as sam_extract = 4
        '000000004': {
            'awardee_or_recipient_uniqu': '000000004',
            'registration_date': '2014-09-29',
            'expiration_date': '2015-09-29',
            'last_sam_mod_date': '2014-09-29',
            'activation_date': '2014-09-29',
            'deactivation_date': None,
            'legal_business_name': 'SPECIAL TEST, OFFICE OF',
            'address_line_1': '1530 X AVE NN, STE 113',
            'address_line_2': None,
            'city': 'WASHINGTON',
            'state': 'DC',
            'zip': '20036',
            'zip4': '4531',
            'country_code': 'USA',
            'congressional_district': '98',
            'business_types_codes': ['2R'],
            'dba_name': None,
            'ultimate_parent_unique_ide': '161906193',
            'ultimate_parent_legal_enti': None,
            'historic': False
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
            'dba_name': duns_obj.dba_name,
            'ultimate_parent_unique_ide': duns_obj.ultimate_parent_unique_ide,
            'ultimate_parent_legal_enti': duns_obj.ultimate_parent_legal_enti,
            'historic': duns_obj.historic
        }
    assert results == expected_results


def test_load_exec_comp(database):
    """ Test a local run load exec_comp with the test files """
    sess = database.session
    duns_dir = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'fake_sam_files', 'duns')
    exec_comp_dir = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'fake_sam_files', 'exec_comp')

    load_duns.process_duns_dir(sess, True, duns_dir)
    load_exec_comp.process_exec_comp_dir(sess, True, exec_comp_dir, None)

    monthly_last_exec_date = datetime.date(2017, 9, 30)
    daily_last_exec_date = datetime.date(2019, 3, 29)

    expected_results = {
        # Not updated as sam_extract = 1
        '000000000': {
            'awardee_or_recipient_uniqu': '000000000',
            'high_comp_officer1_full_na': 'Terence Test',
            'high_comp_officer1_amount': '11952013',
            'high_comp_officer2_full_na': 'Aaron Test',
            'high_comp_officer2_amount': '41161',
            'high_comp_officer3_full_na': 'Jason Test',
            'high_comp_officer3_amount': '286963',
            'high_comp_officer4_full_na': 'Michael Test',
            'high_comp_officer4_amount': '129337',
            'high_comp_officer5_full_na': 'Mark Test',
            'high_comp_officer5_amount': '1248877',
            'last_exec_comp_mod_date': monthly_last_exec_date
        },
        # Updated as sam_extract = 2
        '000000001': {
            'awardee_or_recipient_uniqu': '000000001',
            'high_comp_officer1_full_na': 'Terence Test Updated',
            'high_comp_officer1_amount': '21952013',
            'high_comp_officer2_full_na': 'Aaron Test Updated',
            'high_comp_officer2_amount': '51161',
            'high_comp_officer3_full_na': 'Jason Test Updated',
            'high_comp_officer3_amount': '386963',
            'high_comp_officer4_full_na': 'Michael Test Updated',
            'high_comp_officer4_amount': '329337',
            'high_comp_officer5_full_na': 'Mark Test Updated',
            'high_comp_officer5_amount': '3248877',
            'last_exec_comp_mod_date': daily_last_exec_date
        },
        # Updated as sam_extract = 3
        '000000002': {
            'awardee_or_recipient_uniqu': '000000002',
            'high_comp_officer1_full_na': 'Terence Test Updated',
            'high_comp_officer1_amount': '21952013',
            'high_comp_officer2_full_na': 'Aaron Test Updated',
            'high_comp_officer2_amount': '51161',
            'high_comp_officer3_full_na': 'Jason Test Updated',
            'high_comp_officer3_amount': '386963',
            'high_comp_officer4_full_na': 'Michael Test Updated',
            'high_comp_officer4_amount': '329337',
            'high_comp_officer5_full_na': 'Mark Test Updated',
            'high_comp_officer5_amount': '3248877',
            'last_exec_comp_mod_date': daily_last_exec_date
        },
        # Not updated as sam_extract = 4
        '000000003': {
            'awardee_or_recipient_uniqu': '000000003',
            'high_comp_officer1_full_na': 'Terence Test',
            'high_comp_officer1_amount': '11952013',
            'high_comp_officer2_full_na': 'Aaron Test',
            'high_comp_officer2_amount': '41161',
            'high_comp_officer3_full_na': 'Jason Test',
            'high_comp_officer3_amount': '286963',
            'high_comp_officer4_full_na': 'Michael Test',
            'high_comp_officer4_amount': '129337',
            'high_comp_officer5_full_na': 'Mark Test',
            'high_comp_officer5_amount': '1248877',
            'last_exec_comp_mod_date': monthly_last_exec_date
        },
        # Not updated as sam_extract = 4
        '000000004': {
            'awardee_or_recipient_uniqu': '000000004',
            'high_comp_officer1_full_na': 'Terence Test',
            'high_comp_officer1_amount': '11952013',
            'high_comp_officer2_full_na': 'Aaron Test',
            'high_comp_officer2_amount': '41161',
            'high_comp_officer3_full_na': 'Jason Test',
            'high_comp_officer3_amount': '286963',
            'high_comp_officer4_full_na': 'Michael Test',
            'high_comp_officer4_amount': '129337',
            'high_comp_officer5_full_na': 'Mark Test',
            'high_comp_officer5_amount': '1248877',
            'last_exec_comp_mod_date': monthly_last_exec_date
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
    print(results)
    assert results == expected_results
