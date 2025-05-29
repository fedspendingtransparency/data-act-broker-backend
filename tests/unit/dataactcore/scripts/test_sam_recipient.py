import os
import datetime
import pytest

from dataactcore.config import CONFIG_BROKER
from dataactcore.scripts.pipeline import load_duns_exec_comp
from dataactcore.models.domainModels import SAMRecipient, SAMRecipientUnregistered


def test_load_duns(database):
    """Test a local run load duns with the test files"""
    sess = database.session
    duns_dir = os.path.join(CONFIG_BROKER["path"], "tests", "unit", "data", "fake_sam_files", "duns")

    load_duns_exec_comp.load_from_sam_extract("DUNS", sess, True, duns_dir)

    # update if the fake DUNS file name/zip changes
    deactivation_date = "2021-02-07"

    expected_duns_results = {
        # Pulled active daily V1 record, not updated in V2 files
        "000000005": {
            "uei": None,
            "awardee_or_recipient_uniqu": "000000005",
            "registration_date": "2000-02-01",
            "activation_date": "2000-02-02",
            "expiration_date": "2000-02-25",
            "last_sam_mod_date": "2000-02-15",
            "deactivation_date": None,
            "legal_business_name": "LEGAL BUSINESS NAME 000000005 V1 DAILY",
            "address_line_1": "ADDRESS LINE 1 000000005 V1 DAILY",
            "address_line_2": "ADDRESS LINE 2 000000005 V1 DAILY",
            "city": "CITY 000000005 V1 DAILY",
            "state": "ST 000000005 V1 DAILY",
            "zip": "ZIP 000000005 V1 DAILY",
            "zip4": "ZIP4 000000005 V1 DAILY",
            "country_code": "COUNTRY 000000005 V1 DAILY",
            "congressional_district": "CONGRESSIONAL DISTRICT 000000005 V1 DAILY",
            "business_types_codes": ["2X", "MF"],
            "business_types": ["For Profit Organization", "Manufacturer of Goods"],
            "dba_name": "DBA NAME 000000005 V1 DAILY",
            "ultimate_parent_uei": None,
            "ultimate_parent_unique_ide": "000000007",
            # ultimate_parent_legal_enti derived via missing parent names
            "ultimate_parent_legal_enti": None,
            "historic": False,
        }
    }
    expected_uei_results = {
        # Pulled active monthly record, slightly updated with deactivation date as sam_extract = 1
        # Also present in V2 but only as a delete, so update just the UEI and deactivation date
        "A1": {
            "uei": "A1",
            "awardee_or_recipient_uniqu": "000000001",
            "registration_date": "1999-01-01",
            "activation_date": "1999-01-02",
            "expiration_date": "1999-01-25",
            "last_sam_mod_date": "1999-01-15",
            "deactivation_date": deactivation_date,
            "legal_business_name": "LEGAL BUSINESS NAME 000000001 V1 MONTHLY",
            "address_line_1": "ADDRESS LINE 1 000000001 V1 MONTHLY",
            "address_line_2": "ADDRESS LINE 2 000000001 V1 MONTHLY",
            "city": "CITY 000000001 V1 MONTHLY",
            "state": "ST 000000001 V1 MONTHLY",
            "zip": "ZIP 000000001 V1 MONTHLY",
            "zip4": "ZIP4 000000001 V1 MONTHLY",
            "country_code": "COUNTRY 000000001 V1 MONTHLY",
            "congressional_district": "CONGRESSIONAL DISTRICT 000000001 V1 MONTHLY",
            "business_types_codes": ["2X", "MF"],
            "business_types": ["For Profit Organization", "Manufacturer of Goods"],
            "dba_name": "DBA NAME 000000001 V1 MONTHLY",
            "ultimate_parent_uei": None,
            "ultimate_parent_unique_ide": "000000004",
            "ultimate_parent_legal_enti": "ULTIMATE PARENT LEGAL BUSINESS NAME 000000004 V1 MONTHLY",
            "historic": False,
        },
        # Pulled active monthly record, updated as sam_extract = 2, don't pull in dup delete record
        # Also with UEI from V2 files
        "B2": {
            "uei": "B2",
            "awardee_or_recipient_uniqu": "000000002",
            "registration_date": "2000-02-01",
            "activation_date": "2000-02-02",
            "expiration_date": "2000-02-25",
            "last_sam_mod_date": "2000-02-15",
            "deactivation_date": None,
            "legal_business_name": "LEGAL BUSINESS NAME B2 V2 BLANK DUNS DAILY",
            "address_line_1": "ADDRESS LINE 1 B2 V2 BLANK DUNS DAILY",
            "address_line_2": "ADDRESS LINE 2 B2 V2 BLANK DUNS DAILY",
            "city": "CITY B2 V2 BLANK DUNS DAILY",
            "state": "ST B2 V2 BLANK DUNS DAILY",
            "zip": "ZIP B2 V2 BLANK DUNS DAILY",
            "zip4": "ZIP4 B2 V2 BLANK DUNS DAILY",
            "country_code": "COUNTRY B2 V2 BLANK DUNS DAILY",
            "congressional_district": "CONGRESSIONAL DISTRICT B2 V2 BLANK DUNS DAILY",
            "business_types_codes": ["2X", "MF"],
            "business_types": ["For Profit Organization", "Manufacturer of Goods"],
            "dba_name": "DBA NAME B2 V2 BLANK DUNS DAILY",
            "ultimate_parent_uei": "E5",
            "ultimate_parent_unique_ide": "000000005",
            "ultimate_parent_legal_enti": "ULTIMATE PARENT LEGAL BUSINESS NAME E5 V2 BLANK DUNS DAILY",
            "historic": False,
        },
        # Pulled active monthly record, updated as sam_extract = 3
        # Also with UEI from V2 files
        "C3": {
            "uei": "C3",
            "awardee_or_recipient_uniqu": "000000003",
            "registration_date": "2000-03-01",
            "activation_date": "2000-03-02",
            "expiration_date": "2000-03-25",
            "last_sam_mod_date": "2000-03-15",
            "deactivation_date": None,
            "legal_business_name": "LEGAL BUSINESS NAME C3 V2 BLANK DUNS DAILY",
            "address_line_1": "ADDRESS LINE 1 C3 V2 BLANK DUNS DAILY",
            "address_line_2": "ADDRESS LINE 2 C3 V2 BLANK DUNS DAILY",
            "city": "CITY C3 V2 BLANK DUNS DAILY",
            "state": "ST C3 V2 BLANK DUNS DAILY",
            "zip": "ZIP C3 V2 BLANK DUNS DAILY",
            "zip4": "ZIP4 C3 V2 BLANK DUNS DAILY",
            "country_code": "COUNTRY C3 V2 BLANK DUNS DAILY",
            "congressional_district": "CONGRESSIONAL DISTRICT C3 V2 BLANK DUNS DAILY",
            "business_types_codes": ["2X", "MF"],
            "business_types": ["For Profit Organization", "Manufacturer of Goods"],
            "dba_name": "DBA NAME C3 V2 BLANK DUNS DAILY",
            "ultimate_parent_uei": "F6",
            "ultimate_parent_unique_ide": "000000006",
            "ultimate_parent_legal_enti": "ULTIMATE PARENT LEGAL BUSINESS NAME F6 V2 BLANK DUNS DAILY",
            "historic": False,
        },
        # Pulled active daily V1 record, updated in daily V2 records (non-UEI record does not reflect this)
        # Also with UEI from V2 files
        "D4": {
            "uei": "D4",
            "awardee_or_recipient_uniqu": "000000004",
            "registration_date": "2000-03-01",
            "activation_date": "2000-03-02",
            "expiration_date": "2000-03-25",
            "last_sam_mod_date": "2000-03-15",
            "deactivation_date": None,
            "legal_business_name": "LEGAL BUSINESS NAME D4 V2 BLANK DUNS DAILY",
            "address_line_1": "ADDRESS LINE 1 D4 V2 BLANK DUNS DAILY",
            "address_line_2": "ADDRESS LINE 2 D4 V2 BLANK DUNS DAILY",
            "city": "CITY D4 V2 BLANK DUNS DAILY",
            "state": "ST D4 V2 BLANK DUNS DAILY",
            "zip": "ZIP D4 V2 BLANK DUNS DAILY",
            "zip4": "ZIP4 D4 V2 BLANK DUNS DAILY",
            "country_code": "COUNTRY D4 V2 BLANK DUNS DAILY",
            "congressional_district": "CONGRESSIONAL DISTRICT D4 V2 BLANK DUNS DAILY",
            "business_types_codes": ["2X", "MF"],
            "business_types": ["For Profit Organization", "Manufacturer of Goods"],
            "dba_name": "DBA NAME D4 V2 BLANK DUNS DAILY",
            "ultimate_parent_uei": "G7",
            "ultimate_parent_unique_ide": "000000007",
            "ultimate_parent_legal_enti": "ULTIMATE PARENT LEGAL BUSINESS NAME G7 V2 BLANK DUNS DAILY",
            "historic": False,
        },
    }

    # Ensure duplicates are covered
    expected_recipient_count = 5
    recipient_count = sess.query(SAMRecipient).count()
    assert recipient_count == expected_recipient_count

    duns_results = {}
    # Get DUNS results
    for recipient_obj in sess.query(SAMRecipient).filter(SAMRecipient.uei.is_(None)).all():
        duns_results[recipient_obj.awardee_or_recipient_uniqu] = {
            "uei": recipient_obj.uei,
            "awardee_or_recipient_uniqu": recipient_obj.awardee_or_recipient_uniqu,
            "registration_date": str(recipient_obj.registration_date) if recipient_obj.registration_date else None,
            "activation_date": str(recipient_obj.activation_date) if recipient_obj.activation_date else None,
            "expiration_date": str(recipient_obj.expiration_date) if recipient_obj.expiration_date else None,
            "last_sam_mod_date": str(recipient_obj.last_sam_mod_date) if recipient_obj.last_sam_mod_date else None,
            "deactivation_date": str(recipient_obj.deactivation_date) if recipient_obj.deactivation_date else None,
            "legal_business_name": recipient_obj.legal_business_name,
            "address_line_1": recipient_obj.address_line_1,
            "address_line_2": recipient_obj.address_line_2,
            "city": recipient_obj.city,
            "state": recipient_obj.state,
            "zip": recipient_obj.zip,
            "zip4": recipient_obj.zip4,
            "country_code": recipient_obj.country_code,
            "congressional_district": recipient_obj.congressional_district,
            "business_types_codes": recipient_obj.business_types_codes,
            "business_types": recipient_obj.business_types,
            "dba_name": recipient_obj.dba_name,
            "ultimate_parent_uei": recipient_obj.ultimate_parent_uei,
            "ultimate_parent_unique_ide": recipient_obj.ultimate_parent_unique_ide,
            "ultimate_parent_legal_enti": recipient_obj.ultimate_parent_legal_enti,
            "historic": recipient_obj.historic,
        }
    assert duns_results == expected_duns_results

    uei_results = {}
    # Get UEI results
    for recipient_obj in sess.query(SAMRecipient).filter(SAMRecipient.uei.isnot(None)).all():
        uei_results[recipient_obj.uei] = {
            "uei": recipient_obj.uei,
            "awardee_or_recipient_uniqu": recipient_obj.awardee_or_recipient_uniqu,
            "registration_date": str(recipient_obj.registration_date) if recipient_obj.registration_date else None,
            "activation_date": str(recipient_obj.activation_date) if recipient_obj.activation_date else None,
            "expiration_date": str(recipient_obj.expiration_date) if recipient_obj.expiration_date else None,
            "last_sam_mod_date": str(recipient_obj.last_sam_mod_date) if recipient_obj.last_sam_mod_date else None,
            "deactivation_date": str(recipient_obj.deactivation_date) if recipient_obj.deactivation_date else None,
            "legal_business_name": recipient_obj.legal_business_name,
            "address_line_1": recipient_obj.address_line_1,
            "address_line_2": recipient_obj.address_line_2,
            "city": recipient_obj.city,
            "state": recipient_obj.state,
            "zip": recipient_obj.zip,
            "zip4": recipient_obj.zip4,
            "country_code": recipient_obj.country_code,
            "congressional_district": recipient_obj.congressional_district,
            "business_types_codes": recipient_obj.business_types_codes,
            "business_types": recipient_obj.business_types,
            "dba_name": recipient_obj.dba_name,
            "ultimate_parent_uei": recipient_obj.ultimate_parent_uei,
            "ultimate_parent_unique_ide": recipient_obj.ultimate_parent_unique_ide,
            "ultimate_parent_legal_enti": recipient_obj.ultimate_parent_legal_enti,
            "historic": recipient_obj.historic,
        }
    assert uei_results == expected_uei_results

    # Fail if provided a record with unmatching DUNS and UEI
    error_dir = os.path.join(CONFIG_BROKER["path"], "tests", "unit", "data", "fake_sam_files", "error_files")
    with pytest.raises(ValueError) as resp_except:
        load_duns_exec_comp.load_from_sam_extract("DUNS", sess, True, error_dir)
    expected_error_recps = ["000000001/A1", "000000002/B2"]
    assert str(resp_except.value).startswith(
        "Unable to add/update sam data. " "A record matched on more than one recipient"
    )
    error_recps = [recp.strip()[1:-1] for recp in str(resp_except.value)[77:-1].split(",")]
    assert set(error_recps) == set(expected_error_recps)


def test_load_exec_comp(database):
    """Test a local run load exec_comp with the test files"""
    sess = database.session
    duns_dir = os.path.join(CONFIG_BROKER["path"], "tests", "unit", "data", "fake_sam_files", "duns")
    exec_comp_dir = os.path.join(CONFIG_BROKER["path"], "tests", "unit", "data", "fake_sam_files", "exec_comp")

    load_duns_exec_comp.load_from_sam_extract("DUNS", sess, True, duns_dir)
    load_duns_exec_comp.load_from_sam_extract("Executive Compensation", sess, True, exec_comp_dir, None)

    monthly_last_exec_date = datetime.date(2017, 9, 30)
    first_daily_exec_date = datetime.date(2019, 3, 29)
    last_daily_exec_date = datetime.date(2019, 3, 30)

    expected_duns_results = {
        # not included in any of the exec comp but listed in sam_recipient
        "000000005": {
            "uei": None,
            "awardee_or_recipient_uniqu": "000000005",
            "high_comp_officer1_full_na": None,
            "high_comp_officer1_amount": None,
            "high_comp_officer2_full_na": None,
            "high_comp_officer2_amount": None,
            "high_comp_officer3_full_na": None,
            "high_comp_officer3_amount": None,
            "high_comp_officer4_full_na": None,
            "high_comp_officer4_amount": None,
            "high_comp_officer5_full_na": None,
            "high_comp_officer5_amount": None,
            "last_exec_comp_mod_date": None,
        }
    }
    expected_uei_results = {
        # processed in the monthly, not updated as sam_extract = 1
        "A1": {
            "uei": "A1",
            "awardee_or_recipient_uniqu": "000000001",
            "high_comp_officer1_full_na": "Terence Test 1",
            "high_comp_officer1_amount": "11952013",
            "high_comp_officer2_full_na": "Aaron Test 1",
            "high_comp_officer2_amount": "41161",
            "high_comp_officer3_full_na": "Jason Test 1",
            "high_comp_officer3_amount": "286963",
            "high_comp_officer4_full_na": "Michael Test 1",
            "high_comp_officer4_amount": "129337",
            "high_comp_officer5_full_na": "Mark Test 1",
            "high_comp_officer5_amount": "1248877",
            "last_exec_comp_mod_date": monthly_last_exec_date,
        },
        # processed in the monthly, processed only in first daily as sam_extract = 2
        "B2": {
            "uei": "B2",
            "awardee_or_recipient_uniqu": "000000002",
            "high_comp_officer1_full_na": "Terence Test Updated 1",
            "high_comp_officer1_amount": "21952013",
            "high_comp_officer2_full_na": "Aaron Test Updated 1",
            "high_comp_officer2_amount": "51161",
            "high_comp_officer3_full_na": "Jason Test Updated 1",
            "high_comp_officer3_amount": "386963",
            "high_comp_officer4_full_na": "Michael Test Updated 1",
            "high_comp_officer4_amount": "329337",
            "high_comp_officer5_full_na": "Mark Test Updated 1",
            "high_comp_officer5_amount": "3248877",
            "last_exec_comp_mod_date": first_daily_exec_date,
        },
        # processed in the monthly, processed in both dailies as sam_extract = 3
        "C3": {
            "uei": "C3",
            "awardee_or_recipient_uniqu": "000000003",
            "high_comp_officer1_full_na": "Terence Test Updated 2",
            "high_comp_officer1_amount": "21952013",
            "high_comp_officer2_full_na": "Aaron Test Updated 2",
            "high_comp_officer2_amount": "51161",
            "high_comp_officer3_full_na": "Jason Test Updated 2",
            "high_comp_officer3_amount": "386963",
            "high_comp_officer4_full_na": "Michael Test Updated 2",
            "high_comp_officer4_amount": "329337",
            "high_comp_officer5_full_na": "Mark Test Updated 2",
            "high_comp_officer5_amount": "3248877",
            "last_exec_comp_mod_date": last_daily_exec_date,
        },
        # processed in the monthly, never updated since
        "D4": {
            "uei": "D4",
            "awardee_or_recipient_uniqu": "000000004",
            "high_comp_officer1_full_na": "Terence Test 2",
            "high_comp_officer1_amount": "11952013",
            "high_comp_officer2_full_na": "Aaron Test 2",
            "high_comp_officer2_amount": "41161",
            "high_comp_officer3_full_na": "Jason Test 2",
            "high_comp_officer3_amount": "286963",
            "high_comp_officer4_full_na": "Michael Test 2",
            "high_comp_officer4_amount": "129337",
            "high_comp_officer5_full_na": "Mark Test 2",
            "high_comp_officer5_amount": "1248877",
            "last_exec_comp_mod_date": monthly_last_exec_date,
        },
    }

    # Get DUNS results
    duns_results = {}
    for recipient_obj in sess.query(SAMRecipient).filter(SAMRecipient.uei.is_(None)).all():
        duns_results[recipient_obj.awardee_or_recipient_uniqu] = {
            "uei": recipient_obj.uei,
            "awardee_or_recipient_uniqu": recipient_obj.awardee_or_recipient_uniqu,
            "high_comp_officer1_full_na": recipient_obj.high_comp_officer1_full_na,
            "high_comp_officer1_amount": recipient_obj.high_comp_officer1_amount,
            "high_comp_officer2_full_na": recipient_obj.high_comp_officer2_full_na,
            "high_comp_officer2_amount": recipient_obj.high_comp_officer2_amount,
            "high_comp_officer3_full_na": recipient_obj.high_comp_officer3_full_na,
            "high_comp_officer3_amount": recipient_obj.high_comp_officer3_amount,
            "high_comp_officer4_full_na": recipient_obj.high_comp_officer4_full_na,
            "high_comp_officer4_amount": recipient_obj.high_comp_officer4_amount,
            "high_comp_officer5_full_na": recipient_obj.high_comp_officer5_full_na,
            "high_comp_officer5_amount": recipient_obj.high_comp_officer5_amount,
            "last_exec_comp_mod_date": recipient_obj.last_exec_comp_mod_date,
        }
    assert duns_results == expected_duns_results

    # Get UEI results
    uei_results = {}
    for recipient_obj in sess.query(SAMRecipient).filter(SAMRecipient.uei.isnot(None)).all():
        uei_results[recipient_obj.uei] = {
            "uei": recipient_obj.uei,
            "awardee_or_recipient_uniqu": recipient_obj.awardee_or_recipient_uniqu,
            "high_comp_officer1_full_na": recipient_obj.high_comp_officer1_full_na,
            "high_comp_officer1_amount": recipient_obj.high_comp_officer1_amount,
            "high_comp_officer2_full_na": recipient_obj.high_comp_officer2_full_na,
            "high_comp_officer2_amount": recipient_obj.high_comp_officer2_amount,
            "high_comp_officer3_full_na": recipient_obj.high_comp_officer3_full_na,
            "high_comp_officer3_amount": recipient_obj.high_comp_officer3_amount,
            "high_comp_officer4_full_na": recipient_obj.high_comp_officer4_full_na,
            "high_comp_officer4_amount": recipient_obj.high_comp_officer4_amount,
            "high_comp_officer5_full_na": recipient_obj.high_comp_officer5_full_na,
            "high_comp_officer5_amount": recipient_obj.high_comp_officer5_amount,
            "last_exec_comp_mod_date": recipient_obj.last_exec_comp_mod_date,
        }
    assert uei_results == expected_uei_results


def test_load_unregistered_entities(database):
    """Test a local run load unregistered_entities with the test files"""
    sess = database.session
    entity_csv_dir = os.path.join(
        CONFIG_BROKER["path"], "tests", "unit", "data", "fake_sam_files", "unregistered_entity"
    )

    load_duns_exec_comp.load_from_sam_entity_api(sess, entity_csv_dir)
    expected_results = {
        "UEI000000001": {
            "uei": "UEI000000001",
            "legal_business_name": "TEST UNREGISTERED ENTITY 1",
            "address_line_1": "TEST ADDRESS 1",
            "address_line_2": None,
            "city": "ROSHARON",
            "state": "TX",
            "zip": "77583",
            "zip4": "6527",
            "country_code": "USA",
            "congressional_district": None,
        },
        "UEI000000002": {
            "uei": "UEI000000002",
            "legal_business_name": "TEST UNREGISTERED ENTITY 2",
            "address_line_1": "TEST ADDRESS 2",
            "address_line_2": "TEST ADDRESS 2-2",
            "city": "MEDFORD",
            "state": "OR",
            "zip": "97504",
            "zip4": "2905",
            "country_code": "BGD",
            "congressional_district": None,
        },
        "UEI000000003": {
            "uei": "UEI000000003",
            "legal_business_name": "TEST UNREGISTERED ENTITY 3",
            "address_line_1": "TEST ADDRESS 3",
            "address_line_2": None,
            "city": "Capitola",
            "state": "CA",
            "zip": "95010",
            "zip4": "2645",
            "country_code": "USA",
            "congressional_district": None,
        },
        "UEI000000004": {
            "uei": "UEI000000004",
            "legal_business_name": "TEST UNREGISTERED ENTITY 4",
            "address_line_1": "TEST ADDRESS 4",
            "address_line_2": "TEST ADDRESS 4-2",
            "city": "SUMMERVILLE",
            "state": "SC",
            "zip": "29483",
            "zip4": "3470",
            "country_code": "CAN",
            "congressional_district": None,
        },
    }

    # Get UEI results
    results = {}
    for recipient_obj in sess.query(SAMRecipientUnregistered).all():
        results[recipient_obj.uei] = {
            "uei": recipient_obj.uei,
            "legal_business_name": recipient_obj.legal_business_name,
            "address_line_1": recipient_obj.address_line_1,
            "address_line_2": recipient_obj.address_line_2,
            "city": recipient_obj.city,
            "state": recipient_obj.state,
            "zip": recipient_obj.zip,
            "zip4": recipient_obj.zip4,
            "country_code": recipient_obj.country_code,
            "congressional_district": recipient_obj.congressional_district,
        }
    assert results == expected_results
