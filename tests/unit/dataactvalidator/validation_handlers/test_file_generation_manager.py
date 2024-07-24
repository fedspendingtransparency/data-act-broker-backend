import csv
import os
import re
import pytest

from datetime import datetime, timedelta
from unittest.mock import Mock

from dataactbroker.helpers import generation_helper

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT, PUBLISH_STATUS_DICT
from dataactcore.models.stagingModels import DetachedAwardProcurement, PublishedFABS
from dataactcore.models.domainModels import SF133, concat_tas_dict, concat_display_tas_dict

from dataactvalidator.validation_handlers import file_generation_manager
from dataactvalidator.validation_handlers.file_generation_manager import FileGenerationManager

from tests.unit.dataactcore.factories.job import (JobFactory, FileGenerationFactory, SubmissionFactory,
                                                  SubmissionWindowScheduleFactory)
from tests.unit.dataactcore.factories.domain import (TASFactory, SF133Factory, SAMRecipientFactory, CGACFactory,
                                                     FRECFactory, TASFailedEditsFactory, GTASBOCFactory)
from tests.unit.dataactcore.factories.staging import (AwardFinancialAssistanceFactory, AwardProcurementFactory,
                                                      DetachedAwardProcurementFactory, PublishedFABSFactory,
                                                      PublishedObjectClassProgramActivityFactory)


d1_booleans = ['small_business_competitive', 'city_local_government', 'county_local_government',
               'inter_municipal_local_gove', 'local_government_owned', 'municipality_local_governm',
               'school_district_local_gove', 'township_local_government', 'us_state_government',
               'us_federal_government', 'federal_agency', 'federally_funded_research', 'us_tribal_government',
               'foreign_government', 'community_developed_corpor', 'labor_surplus_area_firm',
               'corporate_entity_not_tax_e', 'corporate_entity_tax_exemp', 'partnership_or_limited_lia',
               'sole_proprietorship', 'small_agricultural_coopera', 'international_organization',
               'us_government_entity', 'emerging_small_business', 'c8a_program_participant',
               'sba_certified_8_a_joint_ve', 'dot_certified_disadvantage', 'self_certified_small_disad',
               'historically_underutilized', 'small_disadvantaged_busine', 'the_ability_one_program',
               'historically_black_college', 'c1862_land_grant_college', 'c1890_land_grant_college',
               'c1994_land_grant_college', 'minority_institution', 'private_university_or_coll', 'school_of_forestry',
               'state_controlled_instituti', 'tribal_college', 'veterinary_college', 'educational_institution',
               'alaskan_native_servicing_i', 'community_development_corp', 'native_hawaiian_servicing',
               'domestic_shelter', 'manufacturer_of_goods', 'hospital_flag', 'veterinary_hospital',
               'hispanic_servicing_institu', 'foundation', 'woman_owned_business', 'minority_owned_business',
               'women_owned_small_business', 'economically_disadvantaged', 'joint_venture_women_owned',
               'joint_venture_economically', 'veteran_owned_business', 'service_disabled_veteran_o', 'contracts',
               'grants', 'receives_contracts_and_gra', 'airport_authority', 'council_of_governments',
               'housing_authorities_public', 'interstate_entity', 'planning_commission', 'port_authority',
               'transit_authority', 'subchapter_s_corporation', 'limited_liability_corporat',
               'foreign_owned_and_located', 'american_indian_owned_busi', 'alaskan_native_owned_corpo',
               'indian_tribe_federally_rec', 'native_hawaiian_owned_busi', 'tribally_owned_business',
               'asian_pacific_american_own', 'black_american_owned_busin', 'hispanic_american_owned_bu',
               'native_american_owned_busi', 'subcontinent_asian_asian_i', 'other_minority_owned_busin',
               'for_profit_organization', 'nonprofit_organization', 'other_not_for_profit_organ', 'us_local_government']


def test_tas_concats():
    # everything
    tas_dict = {
        'allocation_transfer_agency': '097',
        'agency_identifier': '017',
        'beginning_period_of_availa': '2017',
        'ending_period_of_availabil': '2017',
        'availability_type_code': 'D',
        'main_account_code': '0001',
        'sub_account_code': '001'
    }
    tas2_str = concat_tas_dict(tas_dict)
    tas2_dstr = concat_display_tas_dict(tas_dict)
    assert tas2_str == '09701720172017D0001001'
    assert tas2_dstr == '097-017-D-0001-001'

    # everything sans type code
    tas_dict = {
        'allocation_transfer_agency': '097',
        'agency_identifier': '017',
        'beginning_period_of_availa': '2017',
        'ending_period_of_availabil': '2017',
        'availability_type_code': None,
        'main_account_code': '0001',
        'sub_account_code': '001'
    }
    tas2_str = concat_tas_dict(tas_dict)
    tas2_dstr = concat_display_tas_dict(tas_dict)
    assert tas2_str == '09701720172017 0001001'
    assert tas2_dstr == '097-017-2017/2017-0001-001'

    # everything sans ata
    tas_dict = {
        'allocation_transfer_agency': None,
        'agency_identifier': '017',
        'beginning_period_of_availa': '2017',
        'ending_period_of_availabil': '2017',
        'availability_type_code': 'D',
        'main_account_code': '0001',
        'sub_account_code': '001'
    }
    tas2_str = concat_tas_dict(tas_dict)
    tas2_dstr = concat_display_tas_dict(tas_dict)
    assert tas2_str == '00001720172017D0001001'
    assert tas2_dstr == '017-D-0001-001'

    # everything sans aid
    tas_dict = {
        'allocation_transfer_agency': '097',
        'agency_identifier': None,
        'beginning_period_of_availa': '2017',
        'ending_period_of_availabil': '2017',
        'availability_type_code': 'D',
        'main_account_code': '0001',
        'sub_account_code': '001'
    }
    tas2_str = concat_tas_dict(tas_dict)
    tas2_dstr = concat_display_tas_dict(tas_dict)
    assert tas2_str == '09700020172017D0001001'
    assert tas2_dstr == '097-D-0001-001'

    # everything sans periods
    tas_dict = {
        'allocation_transfer_agency': '097',
        'agency_identifier': '017',
        'beginning_period_of_availa': None,
        'ending_period_of_availabil': None,
        'availability_type_code': None,
        'main_account_code': '0001',
        'sub_account_code': '001'
    }
    tas2_str = concat_tas_dict(tas_dict)
    tas2_dstr = concat_display_tas_dict(tas_dict)
    assert tas2_str == '09701700000000 0001001'
    assert tas2_dstr == '097-017-0001-001'

    # everything sans beginning period
    tas_dict = {
        'allocation_transfer_agency': '097',
        'agency_identifier': '017',
        'beginning_period_of_availa': None,
        'ending_period_of_availabil': '2017',
        'availability_type_code': None,
        'main_account_code': '0001',
        'sub_account_code': '001'
    }
    tas2_str = concat_tas_dict(tas_dict)
    tas2_dstr = concat_display_tas_dict(tas_dict)
    assert tas2_str == '09701700002017 0001001'
    assert tas2_dstr == '097-017-2017-0001-001'

    # everything sans codes
    tas_dict = {
        'allocation_transfer_agency': '097',
        'agency_identifier': '017',
        'beginning_period_of_availa': '2017',
        'ending_period_of_availabil': '2017',
        'availability_type_code': 'D',
        'main_account_code': None,
        'sub_account_code': None
    }
    tas2_str = concat_tas_dict(tas_dict)
    tas2_dstr = concat_display_tas_dict(tas_dict)
    assert tas2_str == '09701720172017D0000000'
    assert tas2_dstr == '097-017-D'

    # nothing
    tas_dict = {
        'allocation_transfer_agency': None,
        'agency_identifier': None,
        'beginning_period_of_availa': None,
        'ending_period_of_availabil': None,
        'availability_type_code': None,
        'main_account_code': None,
        'sub_account_code': None
    }
    tas2_str = concat_tas_dict(tas_dict)
    tas2_dstr = concat_display_tas_dict(tas_dict)
    assert tas2_str == '00000000000000 0000000'
    assert tas2_dstr == ''


@pytest.mark.usefixtures("job_constants", "broker_files_tmp_dir")
def test_generate_a(database, monkeypatch):
    sess = database.session
    monkeypatch.setattr(file_generation_manager, 'get_timestamp', Mock(return_value='123456789'))

    agency_cgac = '097'
    year = 2017

    tas1_dict = {
        'allocation_transfer_agency': agency_cgac,
        'agency_identifier': '000',
        'beginning_period_of_availa': '2017',
        'ending_period_of_availabil': '2017',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '001'
    }
    tas1_str = concat_tas_dict(tas1_dict)

    tas2_dict = {
        'allocation_transfer_agency': None,
        'agency_identifier': '017',
        'beginning_period_of_availa': '2017',
        'ending_period_of_availabil': '2017',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '001'
    }
    tas2_str = concat_tas_dict(tas2_dict)

    sf1 = SF133Factory(period=6, fiscal_year=year, tas=tas1_str, line=1160, amount='1.00', **tas1_dict)
    sf2 = SF133Factory(period=6, fiscal_year=year, tas=tas1_str, line=1180, amount='2.00', **tas1_dict)
    sf3 = SF133Factory(period=6, fiscal_year=year, tas=tas2_str, line=1000, amount='4.00', **tas2_dict)
    sf4 = SF133Factory(period=6, fiscal_year=year, tas=tas2_str, line=1042, amount='4.00', **tas2_dict)
    sf5 = SF133Factory(period=6, fiscal_year=year, tas=tas2_str, line=1067, amount='4.00', **tas2_dict)
    tas1 = TASFactory(financial_indicator2=' ', **tas1_dict)
    tas2 = TASFactory(financial_indicator2=' ', **tas2_dict)
    job = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['appropriations'], filename=None, start_date='01/01/2017',
                     end_date='03/31/2017', submission=None)
    sess.add_all([sf1, sf2, sf3, sf4, sf5, tas1, tas2, job])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], job=job)
    # providing agency code here as it will be passed via SQS and detached file jobs don't store agency code
    file_gen_manager.generate_file(agency_cgac)

    assert job.filename == os.path.join(CONFIG_BROKER['broker_files'], 'File-A_FY17P06_123456789.csv')

    # check headers
    file_rows = read_file_rows(job.filename)
    assert file_rows[0] == [val[0] for key, val in file_generation_manager.fileA.mapping.items()]

    # check body
    sf1 = sess.query(SF133).filter_by(tas=tas1_str).first()
    sf2 = sess.query(SF133).filter_by(tas=tas2_str).first()
    expected1 = []
    expected2 = []
    sum_cols = [
        'total_budgetary_resources_cpe',
        'budget_authority_appropria_cpe',
        'budget_authority_unobligat_fyb',
        'adjustments_to_unobligated_cpe',
        'other_budgetary_resources_cpe',
        'contract_authority_amount_cpe',
        'borrowing_authority_amount_cpe',
        'spending_authority_from_of_cpe',
        'status_of_budgetary_resour_cpe',
        'obligations_incurred_total_cpe',
        'gross_outlay_amount_by_tas_cpe',
        'unobligated_balance_cpe',
        'deobligations_recoveries_r_cpe'
    ]
    zero_sum_cols = {sum_col: '0' for sum_col in sum_cols}
    expected1_sum_cols = zero_sum_cols.copy()
    expected1_sum_cols['budget_authority_appropria_cpe'] = '3.00'
    expected2_sum_cols = zero_sum_cols.copy()
    expected2_sum_cols['budget_authority_unobligat_fyb'] = '4.00'
    expected2_sum_cols['adjustments_to_unobligated_cpe'] = '4.00'
    for value in file_generation_manager.fileA.db_columns:
        # loop through all values and format date columns
        if value in sf1.__dict__:
            expected1.append(str(sf1.__dict__[value] or ''))
            expected2.append(str(sf2.__dict__[value] or ''))
        elif value in expected1_sum_cols:
            expected1.append(expected1_sum_cols[value])
            expected2.append(expected2_sum_cols[value])
        elif value == 'gtas_status':
            expected1.append('')
            expected2.append('')

    assert expected1 in file_rows
    assert expected2 in file_rows


@pytest.mark.usefixtures("job_constants", "broker_files_tmp_dir")
def test_generate_a_after_2020(database, monkeypatch):
    sess = database.session
    monkeypatch.setattr(file_generation_manager, 'get_timestamp', Mock(return_value='123456789'))

    agency_cgac = '097'
    year = 2021

    tas_dict = {
        'allocation_transfer_agency': agency_cgac,
        'agency_identifier': '000',
        'beginning_period_of_availa': '2021',
        'ending_period_of_availabil': '2021',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '001'
    }
    tas_str = concat_tas_dict(tas_dict)

    sf1 = SF133Factory(period=6, fiscal_year=year, tas=tas_str, line=1042, amount='4.00', **tas_dict)
    sf2 = SF133Factory(period=6, fiscal_year=year, tas=tas_str, line=1067, amount='4.00', **tas_dict)
    tas = TASFactory(financial_indicator2=' ', **tas_dict)
    job = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['appropriations'], filename=None, start_date='01/01/2021',
                     end_date='03/31/2021', submission=None)
    sess.add_all([sf1, sf2, tas, job])
    sess.commit()

    # First job, prior to 2021
    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], job=job)
    # providing agency code here as it will be passed via SQS and detached file jobs don't store agency code
    file_gen_manager.generate_file(agency_cgac)

    assert job.filename == os.path.join(CONFIG_BROKER['broker_files'], 'File-A_FY21P06_123456789.csv')

    # check headers
    file_rows = read_file_rows(job.filename)
    assert file_rows[0] == [val[0] for key, val in file_generation_manager.fileA.mapping.items()]

    # check body
    sf = sess.query(SF133).filter_by(tas=tas_str).first()
    expected = []
    sum_cols = [
        'total_budgetary_resources_cpe',
        'budget_authority_appropria_cpe',
        'budget_authority_unobligat_fyb',
        'adjustments_to_unobligated_cpe',
        'other_budgetary_resources_cpe',
        'contract_authority_amount_cpe',
        'borrowing_authority_amount_cpe',
        'spending_authority_from_of_cpe',
        'status_of_budgetary_resour_cpe',
        'obligations_incurred_total_cpe',
        'gross_outlay_amount_by_tas_cpe',
        'unobligated_balance_cpe',
        'deobligations_recoveries_r_cpe'
    ]
    zero_sum_cols = {sum_col: '0' for sum_col in sum_cols}
    expected_sum_cols = zero_sum_cols.copy()
    expected_sum_cols['adjustments_to_unobligated_cpe'] = '8.00'
    for value in file_generation_manager.fileA.db_columns:
        # loop through all values and format date columns
        if value in sf.__dict__:
            expected.append(str(sf.__dict__[value] or ''))
        elif value in expected_sum_cols:
            expected.append(expected_sum_cols[value])
        elif value == 'gtas_status':
            expected.append('')

    assert expected in file_rows


@pytest.mark.usefixtures("job_constants", "broker_files_tmp_dir")
def test_generate_a_null_ata(database, monkeypatch):
    sess = database.session
    monkeypatch.setattr(file_generation_manager, 'get_timestamp', Mock(return_value='123456789'))

    agency_cgac = '097'
    agency_frec = '1137'
    year = 2017

    cgac = CGACFactory(cgac_code=agency_cgac)
    frec = FRECFactory(frec_code=agency_frec, cgac=cgac)

    tas1_dict = {
        'allocation_transfer_agency': None,
        'agency_identifier': '011',
        'beginning_period_of_availa': '2017',
        'ending_period_of_availabil': '2017',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '001'
    }
    tas1_str = concat_tas_dict(tas1_dict)

    tas2_dict = {
        'allocation_transfer_agency': None,
        'agency_identifier': '011',
        'beginning_period_of_availa': '2017',
        'ending_period_of_availabil': '2017',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '002'
    }
    tas2_str = concat_tas_dict(tas2_dict)

    sf1 = SF133Factory(period=6, fiscal_year=year, tas=tas1_str, line=1160, amount='1.00', **tas1_dict)
    sf2 = SF133Factory(period=6, fiscal_year=year, tas=tas1_str, line=1180, amount='2.00', **tas1_dict)
    sf3 = SF133Factory(period=6, fiscal_year=year, tas=tas2_str, line=1000, amount='4.00', **tas2_dict)
    sf4 = SF133Factory(period=6, fiscal_year=year, tas=tas2_str, line=1042, amount='4.00', **tas2_dict)
    sf5 = SF133Factory(period=6, fiscal_year=year, tas=tas2_str, line=1067, amount='4.00', **tas2_dict)
    tas1 = TASFactory(financial_indicator2=' ', fr_entity_type=agency_frec, **tas1_dict)
    tas2 = TASFactory(financial_indicator2=' ', fr_entity_type=agency_frec, **tas2_dict)
    job = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['appropriations'], filename=None, start_date='01/01/2017',
                     end_date='03/31/2017', submission=None)
    sess.add_all([cgac, frec, sf1, sf2, sf3, sf4, sf5, tas1, tas2, job])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], job=job)
    # providing agency code here as it will be passed via SQS and detached file jobs don't store agency code
    file_gen_manager.generate_file(agency_cgac)

    assert job.filename == os.path.join(CONFIG_BROKER['broker_files'], 'File-A_FY17P06_123456789.csv')

    # check headers
    file_rows = read_file_rows(job.filename)
    assert file_rows[0] == [val[0] for key, val in file_generation_manager.fileA.mapping.items()]

    # check body
    sf1 = sess.query(SF133).filter_by(tas=tas1_str).first()
    sf2 = sess.query(SF133).filter_by(tas=tas2_str).first()
    expected1 = []
    expected2 = []
    sum_cols = [
        'total_budgetary_resources_cpe',
        'budget_authority_appropria_cpe',
        'budget_authority_unobligat_fyb',
        'adjustments_to_unobligated_cpe',
        'other_budgetary_resources_cpe',
        'contract_authority_amount_cpe',
        'borrowing_authority_amount_cpe',
        'spending_authority_from_of_cpe',
        'status_of_budgetary_resour_cpe',
        'obligations_incurred_total_cpe',
        'gross_outlay_amount_by_tas_cpe',
        'unobligated_balance_cpe',
        'deobligations_recoveries_r_cpe'
    ]
    zero_sum_cols = {sum_col: '0' for sum_col in sum_cols}
    expected1_sum_cols = zero_sum_cols.copy()
    expected1_sum_cols['budget_authority_appropria_cpe'] = '3.00'
    expected2_sum_cols = zero_sum_cols.copy()
    expected2_sum_cols['budget_authority_unobligat_fyb'] = '4.00'
    expected2_sum_cols['adjustments_to_unobligated_cpe'] = '4.00'
    for value in file_generation_manager.fileA.db_columns:
        # loop through all values and format date columns
        if value in sf1.__dict__:
            expected1.append(str(sf1.__dict__[value] or ''))
            expected2.append(str(sf2.__dict__[value] or ''))
        elif value in expected1_sum_cols:
            expected1.append(expected1_sum_cols[value])
            expected2.append(expected2_sum_cols[value])
        elif value == 'gtas_status':
            expected1.append('')
            expected2.append('')

    assert expected1 in file_rows
    assert expected2 in file_rows


@pytest.mark.usefixtures("job_constants", "broker_files_tmp_dir")
def test_generate_a_gtas_status(database, monkeypatch):
    sess = database.session
    monkeypatch.setattr(file_generation_manager, 'get_timestamp', Mock(return_value='123456789'))

    agency_cgac = '097'
    year = 2021

    tas_1_dict = {
        'allocation_transfer_agency': agency_cgac,
        'agency_identifier': '000',
        'beginning_period_of_availa': '2021',
        'ending_period_of_availabil': '2021',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '001'
    }
    tas_1_str = concat_tas_dict(tas_1_dict)

    tas_2_dict = {
        'allocation_transfer_agency': agency_cgac,
        'agency_identifier': '000',
        'beginning_period_of_availa': '2021',
        'ending_period_of_availabil': '2021',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '002'
    }
    tas_2_str = concat_tas_dict(tas_2_dict)

    tas_3_dict = {
        'allocation_transfer_agency': agency_cgac,
        'agency_identifier': '000',
        'beginning_period_of_availa': '2021',
        'ending_period_of_availabil': '2021',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '003'
    }
    tas_3_str = concat_tas_dict(tas_3_dict)

    tas_4_dict = {
        'allocation_transfer_agency': agency_cgac,
        'agency_identifier': '000',
        'beginning_period_of_availa': '2021',
        'ending_period_of_availabil': '2021',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '004'
    }
    tas_4_str = concat_tas_dict(tas_4_dict)

    tas_5_dict = {
        'allocation_transfer_agency': agency_cgac,
        'agency_identifier': '000',
        'beginning_period_of_availa': '2021',
        'ending_period_of_availabil': '2021',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '005'
    }
    tas_5_str = concat_tas_dict(tas_5_dict)

    tas_6_dict = {
        'allocation_transfer_agency': agency_cgac,
        'agency_identifier': '000',
        'beginning_period_of_availa': '2021',
        'ending_period_of_availabil': '2021',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '006'
    }
    tas_6_str = concat_tas_dict(tas_6_dict)

    tas_7_dict = {
        'allocation_transfer_agency': agency_cgac,
        'agency_identifier': '000',
        'beginning_period_of_availa': '2021',
        'ending_period_of_availabil': '2021',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '007'
    }
    tas_7_str = concat_tas_dict(tas_7_dict)

    sf_1 = SF133Factory(period=6, fiscal_year=year, tas=tas_1_str, line=1042, amount='4.00', **tas_1_dict)
    sf_2 = SF133Factory(period=6, fiscal_year=year, tas=tas_2_str, line=1042, amount='4.00', **tas_2_dict)
    sf_3 = SF133Factory(period=6, fiscal_year=year, tas=tas_3_str, line=1042, amount='4.00', **tas_3_dict)
    sf_4 = SF133Factory(period=6, fiscal_year=year, tas=tas_4_str, line=1042, amount='4.00', **tas_4_dict)
    sf_5 = SF133Factory(period=6, fiscal_year=year, tas=tas_5_str, line=1042, amount='4.00', **tas_5_dict)
    sf_6 = SF133Factory(period=6, fiscal_year=year, tas=tas_6_str, line=1042, amount='4.00', **tas_6_dict)
    # This one isn't in the "failed" list but does exist in file A and the period exists
    sf_7 = SF133Factory(period=6, fiscal_year=year, tas=tas_7_str, line=1042, amount='4.00', **tas_7_dict)
    # These are ones that don't even have the period in the failed list
    sf_8 = SF133Factory(period=9, fiscal_year=year, tas=tas_1_str, line=1042, amount='4.00', **tas_1_dict)
    sf_9 = SF133Factory(period=7, fiscal_year=year, tas=tas_1_str, line=1042, amount='4.00', **tas_1_dict)
    sf_10 = SF133Factory(period=5, fiscal_year=year, tas=tas_1_str, line=1042, amount='4.00', **tas_1_dict)
    tas_1 = TASFactory(financial_indicator2=' ', **tas_1_dict)
    tas_2 = TASFactory(financial_indicator2=' ', **tas_2_dict)
    tas_3 = TASFactory(financial_indicator2=' ', **tas_3_dict)
    tas_4 = TASFactory(financial_indicator2=' ', **tas_4_dict)
    tas_5 = TASFactory(financial_indicator2=' ', **tas_5_dict)
    tas_6 = TASFactory(financial_indicator2=' ', **tas_6_dict)
    tas_7 = TASFactory(financial_indicator2=' ', **tas_7_dict)
    fail_1 = TASFailedEditsFactory(period=6, fiscal_year=year, tas=tas_1_str, severity='Fatal',
                                   approved_override_exists=False)
    fail_2 = TASFailedEditsFactory(period=6, fiscal_year=year, tas=tas_2_str, severity='Fatal',
                                   approved_override_exists=True, atb_submission_status='F')
    fail_3 = TASFailedEditsFactory(period=6, fiscal_year=year, tas=tas_3_str, severity='Fatal',
                                   approved_override_exists=True, atb_submission_status='E')
    fail_4 = TASFailedEditsFactory(period=6, fiscal_year=year, tas=tas_4_str, severity='Fatal',
                                   approved_override_exists=True, atb_submission_status='P')
    fail_5 = TASFailedEditsFactory(period=6, fiscal_year=year, tas=tas_5_str, severity='Fatal',
                                   approved_override_exists=True, atb_submission_status='C')
    fail_6 = TASFailedEditsFactory(period=6, fiscal_year=year, tas=tas_6_str, severity='Fatal',
                                   approved_override_exists=True)
    # Submission with no fail data available
    sub_window_no_fail = SubmissionWindowScheduleFactory(period=5, year=2021,
                                                         period_start=datetime.now().date() - timedelta(days=2))
    # Submission window that has started (GTAS window closed)
    sub_window_over = SubmissionWindowScheduleFactory(period=6, year=2021,
                                                      period_start=datetime.now().date() - timedelta(days=2))
    # Submission window that hasn't started (GTAS window still open)
    sub_window_progress = SubmissionWindowScheduleFactory(period=7, year=2021,
                                                          period_start=datetime.now().date() + timedelta(days=2))
    # Valid job with a closed GTAS window
    job_1 = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                       file_type_id=FILE_TYPE_DICT['appropriations'], filename=None, start_date='01/01/2021',
                       end_date='03/31/2021', submission=None)
    # Job with no associated submission period
    job_2 = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                       file_type_id=FILE_TYPE_DICT['appropriations'], filename=None, start_date='01/01/2021',
                       end_date='06/30/2021', submission=None)
    # Job with GTAS window still open
    job_3 = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                       file_type_id=FILE_TYPE_DICT['appropriations'], filename=None, start_date='01/01/2021',
                       end_date='04/30/2021', submission=None)
    # Job with no fail data in the database
    job_4 = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                       file_type_id=FILE_TYPE_DICT['appropriations'], filename=None, start_date='01/01/2021',
                       end_date='02/28/2021', submission=None)
    sess.add_all([sf_1, sf_2, sf_3, sf_4, sf_5, sf_6, sf_7, sf_8, sf_9, sf_10, tas_1, tas_2, tas_3, tas_4, tas_5, tas_6,
                  tas_7, fail_1, fail_2, fail_3, fail_4, fail_5, fail_6, sub_window_no_fail, sub_window_over,
                  sub_window_progress, job_1, job_2, job_3, job_4])
    sess.commit()

    # First job, has failed edits to compare to
    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], job=job_1)
    # providing agency code here as it will be passed via SQS and detached file jobs don't store agency code
    file_gen_manager.generate_file(agency_cgac)

    # Second job, no submission period available
    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], job=job_2)
    file_gen_manager.generate_file(agency_cgac)

    # Third job, submission window still open
    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], job=job_3)
    file_gen_manager.generate_file(agency_cgac)

    # Fourth job, no fail data
    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], job=job_4)
    file_gen_manager.generate_file(agency_cgac)

    file_rows_1 = read_file_rows(job_1.filename)
    file_rows_2 = read_file_rows(job_2.filename)
    file_rows_3 = read_file_rows(job_3.filename)
    file_rows_4 = read_file_rows(job_4.filename)

    # check body
    sf_1 = sess.query(SF133).filter_by(tas=tas_1_str).first()
    sf_2 = sess.query(SF133).filter_by(tas=tas_2_str).first()
    sf_3 = sess.query(SF133).filter_by(tas=tas_3_str).first()
    sf_4 = sess.query(SF133).filter_by(tas=tas_4_str).first()
    sf_5 = sess.query(SF133).filter_by(tas=tas_5_str).first()
    sf_6 = sess.query(SF133).filter_by(tas=tas_6_str).first()
    sf_7 = sess.query(SF133).filter_by(tas=tas_7_str).first()
    expected_1 = []
    expected_2 = []
    expected_3 = []
    expected_4 = []
    expected_5 = []
    expected_6 = []
    expected_7 = []
    # Start of checks with no failed entries
    expected_8 = []
    expected_9 = []
    expected_10 = []
    sum_cols = [
        'total_budgetary_resources_cpe',
        'budget_authority_appropria_cpe',
        'budget_authority_unobligat_fyb',
        'adjustments_to_unobligated_cpe',
        'other_budgetary_resources_cpe',
        'contract_authority_amount_cpe',
        'borrowing_authority_amount_cpe',
        'spending_authority_from_of_cpe',
        'status_of_budgetary_resour_cpe',
        'obligations_incurred_total_cpe',
        'gross_outlay_amount_by_tas_cpe',
        'unobligated_balance_cpe',
        'deobligations_recoveries_r_cpe'
    ]
    zero_sum_cols = {sum_col: '0' for sum_col in sum_cols}
    expected_sum_cols = zero_sum_cols.copy()
    expected_sum_cols['adjustments_to_unobligated_cpe'] = '4.00'
    for value in file_generation_manager.fileA.db_columns:
        # loop through all values and format date columns
        if value in sf_1.__dict__:
            expected_1.append(str(sf_1.__dict__[value] or ''))
            expected_2.append(str(sf_2.__dict__[value] or ''))
            expected_3.append(str(sf_3.__dict__[value] or ''))
            expected_4.append(str(sf_4.__dict__[value] or ''))
            expected_5.append(str(sf_5.__dict__[value] or ''))
            expected_6.append(str(sf_6.__dict__[value] or ''))
            expected_7.append(str(sf_7.__dict__[value] or ''))
            expected_8.append(str(sf_1.__dict__[value] or ''))
            expected_9.append(str(sf_1.__dict__[value] or ''))
            expected_10.append(str(sf_1.__dict__[value] or ''))
        elif value in expected_sum_cols:
            expected_1.append(expected_sum_cols[value])
            expected_2.append(expected_sum_cols[value])
            expected_3.append(expected_sum_cols[value])
            expected_4.append(expected_sum_cols[value])
            expected_5.append(expected_sum_cols[value])
            expected_6.append(expected_sum_cols[value])
            expected_7.append(expected_sum_cols[value])
            expected_8.append(expected_sum_cols[value])
            expected_9.append(expected_sum_cols[value])
            expected_10.append(expected_sum_cols[value])
        elif value == 'gtas_status':
            expected_1.append('failed fatal edit - no override')
            expected_2.append('failed fatal edit - override')
            expected_3.append('passed required edits - override')
            expected_4.append('pending certification - override')
            expected_5.append('certified - override')
            expected_6.append('passed required edits - override')
            expected_7.append('passed required edits')
            expected_8.append('')
            expected_9.append('GTAS window open')
            expected_10.append('')

    print(file_rows_1)

    assert expected_1 in file_rows_1
    assert expected_2 in file_rows_1
    assert expected_3 in file_rows_1
    assert expected_4 in file_rows_1
    assert expected_5 in file_rows_1
    assert expected_6 in file_rows_1
    assert expected_7 in file_rows_1
    assert expected_8 in file_rows_2
    assert expected_9 in file_rows_3
    assert expected_10 in file_rows_4


@pytest.mark.usefixtures("job_constants", "broker_files_tmp_dir")
def test_generate_boc(database, monkeypatch):
    sess = database.session
    monkeypatch.setattr(file_generation_manager, 'get_timestamp', Mock(return_value='123456789'))

    agency_cgac = '097'
    year = 2017
    sub_id = 99

    tas1_dict = {
        'allocation_transfer_agency': agency_cgac,
        'agency_identifier': '000',
        'beginning_period_of_availa': '2017',
        'ending_period_of_availabil': '2017',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '001'
    }
    tas1_str = concat_display_tas_dict(tas1_dict)

    tas2_dict = {
        'allocation_transfer_agency': None,
        'agency_identifier': '017',
        'beginning_period_of_availa': '2017',
        'ending_period_of_availabil': '2017',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '001'
    }
    tas2_str = concat_display_tas_dict(tas2_dict)

    # Testing for a TAS that exists in BOC but not in file B
    tas3_dict = {
        'allocation_transfer_agency': agency_cgac,
        'agency_identifier': '000',
        'beginning_period_of_availa': '2017',
        'ending_period_of_availabil': '2017',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '002'
    }
    tas3_str = concat_display_tas_dict(tas3_dict)

    # TAS that shouldn't be bucketed with this agency but exists in BOC
    tas4_dict = {
        'allocation_transfer_agency': '111',
        'agency_identifier': '000',
        'beginning_period_of_availa': '2017',
        'ending_period_of_availabil': '2017',
        'availability_type_code': ' ',
        'main_account_code': '0001',
        'sub_account_code': '002'
    }
    tas4_str = concat_display_tas_dict(tas4_dict)

    tas1 = TASFactory(**tas1_dict)
    tas2 = TASFactory(**tas2_dict)
    tas3 = TASFactory(**tas3_dict)
    tas4 = TASFactory(**tas4_dict)

    # The first two will be combined into a single row, one credit and one debit. The third is a different begin_end
    # indicator, so it will be a separate row. The fourth is a different USSGL number entirely. The fifth is a different
    # TAS. The sixth is a different period and should not be used at all despite matching exactly with the 5th otherwise
    boc1 = GTASBOCFactory(period=6, fiscal_year=year, display_tas=tas1_str, dollar_amount=1.5, ussgl_number='480100',
                          begin_end='B', debit_credit='D', disaster_emergency_fund_code='Q', budget_object_class='1110',
                          reimbursable_flag='D', prior_year_adjustment_code='X', **tas1_dict)
    boc2 = GTASBOCFactory(period=6, fiscal_year=year, display_tas=tas1_str, dollar_amount=2, ussgl_number='480100',
                          begin_end='B', debit_credit='C', disaster_emergency_fund_code='Q', budget_object_class='1110',
                          reimbursable_flag='D', prior_year_adjustment_code='X', **tas1_dict)
    boc3 = GTASBOCFactory(period=6, fiscal_year=year, display_tas=tas1_str, dollar_amount=2, ussgl_number='480100',
                          begin_end='E', debit_credit='D', disaster_emergency_fund_code='Q', budget_object_class='1110',
                          reimbursable_flag='D', prior_year_adjustment_code=None, **tas1_dict)
    boc4 = GTASBOCFactory(period=6, fiscal_year=year, display_tas=tas1_str, dollar_amount=2, ussgl_number='487100',
                          begin_end='E', debit_credit='D', disaster_emergency_fund_code='Q', budget_object_class='1110',
                          reimbursable_flag='D', prior_year_adjustment_code='X', **tas1_dict)
    boc5 = GTASBOCFactory(period=6, fiscal_year=year, display_tas=tas2_str, dollar_amount=25, ussgl_number='480100',
                          begin_end='B', debit_credit='D', disaster_emergency_fund_code='Q', budget_object_class='1110',
                          reimbursable_flag='D', prior_year_adjustment_code=None, **tas2_dict)
    # Different period, should be ignored
    boc6 = GTASBOCFactory(period=7, fiscal_year=year, display_tas=tas2_str, dollar_amount=4, ussgl_number='480100',
                          begin_end='B', debit_credit='D', disaster_emergency_fund_code='Q', budget_object_class='1110',
                          reimbursable_flag='D', prior_year_adjustment_code='X', **tas2_dict)
    # TAS that doesn't exist in file B but does exist in BOC report
    boc7 = GTASBOCFactory(period=6, fiscal_year=year, display_tas=tas3_str, dollar_amount=4, ussgl_number='480100',
                          begin_end='B', debit_credit='D', disaster_emergency_fund_code='Q', budget_object_class='1110',
                          reimbursable_flag='D', prior_year_adjustment_code='X', **tas3_dict)
    # This has a non-X, non-null PYA and should be ignored
    boc8 = GTASBOCFactory(period=6, fiscal_year=year, display_tas=tas1_str, dollar_amount=2, ussgl_number='480100',
                          begin_end='B', debit_credit='C', disaster_emergency_fund_code='Q', budget_object_class='1110',
                          reimbursable_flag='D', prior_year_adjustment_code='Y', **tas1_dict)
    # This is for a TAS that doesn't fit in the bucket. Should be ignored
    boc9 = GTASBOCFactory(period=6, fiscal_year=year, display_tas=tas4_str, dollar_amount=1.5, ussgl_number='480100',
                          begin_end='B', debit_credit='D', disaster_emergency_fund_code='Q', budget_object_class='1110',
                          reimbursable_flag='D', prior_year_adjustment_code='X', **tas4_dict)

    # The first two will end up in the same place, there is an implied "different PAC/PAN"
    pub_b1 = PublishedObjectClassProgramActivityFactory(submission_id=sub_id, display_tas=tas1_str,
                                                        disaster_emergency_fund_code='Q', object_class='111',
                                                        ussgl480100_undelivered_or_fyb=-0.5,
                                                        ussgl480100_undelivered_or_cpe=1.5,
                                                        ussgl487100_downward_adjus_cpe=8,
                                                        by_direct_reimbursable_fun='D', row_number=1, **tas1_dict)
    pub_b2 = PublishedObjectClassProgramActivityFactory(submission_id=sub_id, display_tas=tas1_str,
                                                        disaster_emergency_fund_code='Q', object_class='111',
                                                        ussgl480100_undelivered_or_fyb=0,
                                                        ussgl480100_undelivered_or_cpe=0.5,
                                                        ussgl487100_downward_adjus_cpe=-4,
                                                        by_direct_reimbursable_fun='D', row_number=5, **tas1_dict)
    pub_b3 = PublishedObjectClassProgramActivityFactory(submission_id=sub_id, display_tas=tas2_str,
                                                        disaster_emergency_fund_code='Q', object_class='111',
                                                        ussgl480100_undelivered_or_fyb=0,
                                                        ussgl480100_undelivered_or_cpe=13,
                                                        ussgl487100_downward_adjus_cpe=-4,
                                                        by_direct_reimbursable_fun='D', row_number=9, **tas2_dict)

    sub = SubmissionFactory(submission_id=sub_id, reporting_fiscal_year=year, reporting_fiscal_period=6,
                            publish_status_id=PUBLISH_STATUS_DICT['published'], cgac_code=agency_cgac)
    job = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['boc_comparison'], filename=None, start_date='03/01/2017',
                     end_date='03/31/2017', submission=None)
    sess.add_all([tas1, tas2, tas3, tas4, boc1, boc2, boc3, boc4, boc5, boc6, boc7, boc8, boc9, sub, job, pub_b1,
                  pub_b2, pub_b3])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], job=job)
    # providing agency code here as it will be passed via SQS and detached file jobs don't store agency code
    file_gen_manager.generate_file(agency_cgac)

    assert job.filename == os.path.join(CONFIG_BROKER['broker_files'], 'BOC-Comparison_Report_FY17P06_123456789.csv')

    # check headers
    file_rows = read_file_rows(job.filename)
    assert file_rows[0] == [val[0] for key, val in file_generation_manager.fileBOC.mapping.items()]

    # Removing the file B rows for testing purposes because there's no way to know what order they'll show up in
    for row in file_rows:
        row.pop()

    # check body
    # BOC 1 and 2 and published B 1 and 2
    expected1 = [tas1_str] + list(tas1_dict.values()) +\
                [boc1.budget_object_class, 'D', 'Q', '', boc1.begin_end, str(year), '6', boc1.ussgl_number, '-0.5',
                 '-0.5', '0.0']
    # TAS 2
    tas2_dict['allocation_transfer_agency'] = ''
    expected2 = [tas2_str] + list(tas2_dict.values()) +\
                [boc5.budget_object_class, 'D', 'Q', '', boc5.begin_end, str(year), '6', boc5.ussgl_number, '25',
                 '0', '25']

    # TAS 3
    expected3 = [tas3_str] + list(tas3_dict.values()) + \
                [boc7.budget_object_class, 'D', 'Q', '', boc7.begin_end, str(year), '6', boc7.ussgl_number, '4',
                 '0', '4']

    # TAS 4
    expected4 = [tas4_str] + list(tas4_dict.values()) + \
                [boc9.budget_object_class, 'D', 'Q', '', boc9.begin_end, str(year), '6', boc9.ussgl_number, '1.5',
                 '0', '1.5']

    assert expected1 in file_rows
    assert expected2 in file_rows
    assert expected3 in file_rows
    assert expected4 not in file_rows


@pytest.mark.usefixtures("job_constants", "broker_files_tmp_dir")
def test_generate_sub_d1(database, monkeypatch):
    sess = database.session
    monkeypatch.setattr(file_generation_manager, 'get_timestamp', Mock(return_value='123456789'))

    dap_model = DetachedAwardProcurementFactory
    dap_1 = dap_model(awarding_agency_code='123', action_date='20170101', detached_award_proc_unique='unique1')
    dap_2 = dap_model(awarding_agency_code='123', action_date='20170131', detached_award_proc_unique='unique2')
    dap_3 = dap_model(awarding_agency_code='123', action_date='20170201', detached_award_proc_unique='unique3')
    dap_4 = dap_model(awarding_agency_code='123', action_date='20161231', detached_award_proc_unique='unique4')
    dap_5 = dap_model(awarding_agency_code='234', action_date='20170115', detached_award_proc_unique='unique5')
    file_gen = FileGenerationFactory(request_date=datetime.now().date(), start_date='01/01/2017', end_date='01/31/2017',
                                     file_type='D1', agency_code='123', agency_type='awarding', is_cached_file=False,
                                     file_path=None, file_format='csv')
    sub = SubmissionFactory(submission_id=4, reporting_fiscal_year='2022', reporting_fiscal_period='4')
    job = JobFactory(submission=sub, file_type_id=FILE_TYPE_DICT['award_procurement'])
    sess.add_all([dap_1, dap_2, dap_3, dap_4, dap_5, file_gen, sub, job])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], file_generation=file_gen, job=job)
    file_gen_manager.generate_file()

    assert file_gen.file_path == os.path.join(CONFIG_BROKER['broker_files'],
                                              'SubID-4_File-D1_FY22P04_20170101_20170131_awarding_123456789.csv')


@pytest.mark.usefixtures("job_constants", "broker_files_tmp_dir")
def test_generate_sub_d2(database, monkeypatch):
    sess = database.session
    monkeypatch.setattr(file_generation_manager, 'get_timestamp', Mock(return_value='123456789'))

    dap_model = DetachedAwardProcurementFactory
    dap_1 = dap_model(awarding_agency_code='123', action_date='20170101', detached_award_proc_unique='unique1')
    dap_2 = dap_model(awarding_agency_code='123', action_date='20170131', detached_award_proc_unique='unique2')
    dap_3 = dap_model(awarding_agency_code='123', action_date='20170201', detached_award_proc_unique='unique3')
    dap_4 = dap_model(awarding_agency_code='123', action_date='20161231', detached_award_proc_unique='unique4')
    dap_5 = dap_model(awarding_agency_code='234', action_date='20170115', detached_award_proc_unique='unique5')
    file_gen = FileGenerationFactory(request_date=datetime.now().date(), start_date='01/01/2017', end_date='01/31/2017',
                                     file_type='D1', agency_code='123', agency_type='funding', is_cached_file=False,
                                     file_path=None, file_format='txt')
    sub = SubmissionFactory(submission_id=4, reporting_fiscal_year='2022', reporting_fiscal_period='4')
    job = JobFactory(submission=sub, file_type_id=FILE_TYPE_DICT['award'])
    sess.add_all([dap_1, dap_2, dap_3, dap_4, dap_5, file_gen, sub, job])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], file_generation=file_gen, job=job)
    file_gen_manager.generate_file()

    assert file_gen.file_path == os.path.join(CONFIG_BROKER['broker_files'],
                                              'SubID-4_File-D2_FY22P04_20170101_20170131_funding_123456789.txt')


@pytest.mark.usefixtures("job_constants", "broker_files_tmp_dir")
def test_generate_awarding_d1(database, monkeypatch):
    sess = database.session
    monkeypatch.setattr(file_generation_manager, 'get_timestamp', Mock(return_value='123456789'))

    dap_model = DetachedAwardProcurementFactory
    dap_1 = dap_model(awarding_agency_code='123', action_date='20170101', detached_award_proc_unique='unique1')
    dap_2 = dap_model(awarding_agency_code='123', action_date='20170131', detached_award_proc_unique='unique2')
    dap_3 = dap_model(awarding_agency_code='123', action_date='20170201', detached_award_proc_unique='unique3')
    dap_4 = dap_model(awarding_agency_code='123', action_date='20161231', detached_award_proc_unique='unique4')
    dap_5 = dap_model(awarding_agency_code='234', action_date='20170115', detached_award_proc_unique='unique5')
    file_gen = FileGenerationFactory(request_date=datetime.now().date(), start_date='01/01/2017', end_date='01/31/2017',
                                     file_type='D1', agency_code='123', agency_type='awarding', is_cached_file=True,
                                     file_path=None, file_format='csv')
    sess.add_all([dap_1, dap_2, dap_3, dap_4, dap_5, file_gen])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], file_generation=file_gen)
    file_gen_manager.generate_file()

    assert file_gen.file_path == os.path.join(CONFIG_BROKER['broker_files'],
                                              'File-D1_20170101_20170131_awarding_123456789.csv')

    # check headers
    file_rows = read_file_rows(file_gen.file_path)
    assert file_rows[0] == [val[0] for key, val in file_generation_manager.fileD1.mapping.items()]

    # check body
    dap_one = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique1').first()
    dap_two = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique2').first()
    expected1, expected2 = [], []
    for value in file_generation_manager.fileD1.db_columns:
        # loop through all values and format date columns
        if value in ['period_of_performance_star', 'period_of_performance_curr', 'period_of_perf_potential_e',
                     'ordering_period_end_date', 'action_date', 'last_modified', 'solicitation_date']:
            expected1.append(re.sub(r"[-]", r"", str(dap_one.__dict__[value]))[0:8])
            expected2.append(re.sub(r"[-]", r"", str(dap_two.__dict__[value]))[0:8])
        elif value in d1_booleans:
            expected1.append(str(dap_one.__dict__[value])[0:1].lower() if dap_one.__dict__[value] is not None else None)
            expected2.append(str(dap_two.__dict__[value])[0:1].lower() if dap_two.__dict__[value] is not None else None)
        else:
            expected1.append(str(dap_one.__dict__[value]))
            expected2.append(str(dap_two.__dict__[value]))

    assert expected1 in file_rows
    assert expected2 in file_rows


@pytest.mark.usefixtures("job_constants", "broker_files_tmp_dir")
def test_generate_awarding_d1_alternate_headers(database, monkeypatch):
    sess = database.session
    monkeypatch.setattr(file_generation_manager, 'get_timestamp', Mock(return_value='123456789'))

    dap_model = DetachedAwardProcurementFactory
    dap_1 = dap_model(awarding_agency_code='123', action_date='20170101', detached_award_proc_unique='unique1')
    dap_2 = dap_model(awarding_agency_code='123', action_date='20170131', detached_award_proc_unique='unique2')
    dap_3 = dap_model(awarding_agency_code='123', action_date='20170201', detached_award_proc_unique='unique3')
    dap_4 = dap_model(awarding_agency_code='123', action_date='20161231', detached_award_proc_unique='unique4')
    dap_5 = dap_model(awarding_agency_code='234', action_date='20170115', detached_award_proc_unique='unique5')
    file_gen = FileGenerationFactory(request_date=datetime.now().date(), start_date='01/01/2017', end_date='01/31/2017',
                                     file_type='D1', agency_code='123', agency_type='awarding', is_cached_file=True,
                                     file_path=None, file_format='csv', element_numbers=True)
    sess.add_all([dap_1, dap_2, dap_3, dap_4, dap_5, file_gen])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], file_generation=file_gen)
    file_gen_manager.generate_file()

    assert file_gen.file_path == os.path.join(CONFIG_BROKER['broker_files'],
                                              'File-D1_20170101_20170131_awarding_123456789.csv')

    # check headers
    file_rows = read_file_rows(file_gen.file_path)
    assert file_rows[0] == [val[1] for key, val in file_generation_manager.fileD1.mapping.items()]

    # check body
    dap_one = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique1').first()
    dap_two = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique2').first()
    expected1, expected2 = [], []
    for value in file_generation_manager.fileD1.db_columns:
        # loop through all values and format date columns
        if value in ['period_of_performance_star', 'period_of_performance_curr', 'period_of_perf_potential_e',
                     'ordering_period_end_date', 'action_date', 'last_modified', 'solicitation_date']:
            expected1.append(re.sub(r"[-]", r"", str(dap_one.__dict__[value]))[0:8])
            expected2.append(re.sub(r"[-]", r"", str(dap_two.__dict__[value]))[0:8])
        elif value in d1_booleans:
            expected1.append(str(dap_one.__dict__[value])[0:1].lower() if dap_one.__dict__[value] is not None else None)
            expected2.append(str(dap_two.__dict__[value])[0:1].lower() if dap_two.__dict__[value] is not None else None)
        else:
            expected1.append(str(dap_one.__dict__[value]))
            expected2.append(str(dap_two.__dict__[value]))

    assert expected1 in file_rows
    assert expected2 in file_rows


@pytest.mark.usefixtures("job_constants", "broker_files_tmp_dir")
def test_generate_funding_d1(database, monkeypatch):
    sess = database.session
    monkeypatch.setattr(file_generation_manager, 'get_timestamp', Mock(return_value='123456789'))

    dap_model = DetachedAwardProcurementFactory
    dap_1 = dap_model(funding_agency_code='123', action_date='20170101', detached_award_proc_unique='unique1')
    dap_2 = dap_model(funding_agency_code='123', action_date='20170131', detached_award_proc_unique='unique2')
    dap_3 = dap_model(funding_agency_code='123', action_date='20170201', detached_award_proc_unique='unique3')
    dap_4 = dap_model(funding_agency_code='123', action_date='20161231', detached_award_proc_unique='unique4')
    dap_5 = dap_model(funding_agency_code='234', action_date='20170115', detached_award_proc_unique='unique5')
    file_gen = FileGenerationFactory(request_date=datetime.now().date(), start_date='01/01/2017', end_date='01/31/2017',
                                     file_type='D1', agency_code='123', agency_type='funding', is_cached_file=True,
                                     file_path=None, file_format='csv')
    sess.add_all([dap_1, dap_2, dap_3, dap_4, dap_5, file_gen])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], file_generation=file_gen)
    file_gen_manager.generate_file()

    assert file_gen.file_path == os.path.join(CONFIG_BROKER['broker_files'],
                                              'File-D1_20170101_20170131_funding_123456789.csv')

    # check headers
    file_rows = read_file_rows(file_gen.file_path)
    assert file_rows[0] == [val[0] for key, val in file_generation_manager.fileD1.mapping.items()]

    # check body
    dap_one = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique1').first()
    dap_two = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique2').first()
    expected1, expected2 = [], []
    for value in file_generation_manager.fileD1.db_columns:
        # loop through all values and format date columns
        if value in ['period_of_performance_star', 'period_of_performance_curr', 'period_of_perf_potential_e',
                     'ordering_period_end_date', 'action_date', 'last_modified', 'solicitation_date']:
            expected1.append(re.sub(r"[-]", r"", str(dap_one.__dict__[value]))[0:8])
            expected2.append(re.sub(r"[-]", r"", str(dap_two.__dict__[value]))[0:8])
        elif value in d1_booleans:
            expected1.append(str(dap_one.__dict__[value])[0:1].lower() if dap_one.__dict__[value] is not None else None)
            expected2.append(str(dap_two.__dict__[value])[0:1].lower() if dap_two.__dict__[value] is not None else None)
        else:
            expected1.append(str(dap_one.__dict__[value]))
            expected2.append(str(dap_two.__dict__[value]))

    assert expected1 in file_rows
    assert expected2 in file_rows


@pytest.mark.usefixtures("job_constants", "broker_files_tmp_dir")
def test_generate_awarding_d2(database, monkeypatch):
    sess = database.session
    monkeypatch.setattr(file_generation_manager, 'get_timestamp', Mock(return_value='123456789'))

    pub_fabs_1 = PublishedFABSFactory(awarding_agency_code='123', action_date='20170101',
                                      afa_generated_unique='unique1', is_active=True)
    pub_fabs_2 = PublishedFABSFactory(awarding_agency_code='123', action_date='20170131',
                                      afa_generated_unique='unique2', is_active=True)
    pub_fabs_3 = PublishedFABSFactory(awarding_agency_code='123', action_date='20161231',
                                      afa_generated_unique='unique3', is_active=True)
    pub_fabs_4 = PublishedFABSFactory(awarding_agency_code='123', action_date='20170201',
                                      afa_generated_unique='unique4', is_active=True)
    pub_fabs_5 = PublishedFABSFactory(awarding_agency_code='123', action_date='20170115',
                                      afa_generated_unique='unique5', is_active=False)
    pub_fabs_6 = PublishedFABSFactory(awarding_agency_code='234', action_date='20170115',
                                      afa_generated_unique='unique6', is_active=True)
    file_gen = FileGenerationFactory(request_date=datetime.now().date(), start_date='01/01/2017', end_date='01/31/2017',
                                     file_type='D2', agency_code='123', agency_type='awarding', is_cached_file=True,
                                     file_path=None, file_format='csv')
    sess.add_all([pub_fabs_1, pub_fabs_2, pub_fabs_3, pub_fabs_4, pub_fabs_5, pub_fabs_6, file_gen])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], file_generation=file_gen)
    file_gen_manager.generate_file()

    assert file_gen.file_path == os.path.join(CONFIG_BROKER['broker_files'],
                                              'File-D2_20170101_20170131_awarding_123456789.csv')

    # check headers
    file_rows = read_file_rows(file_gen.file_path)
    assert file_rows[0] == [val[0] for key, val in file_generation_manager.fileD2.mapping.items()]

    # check body
    pub_fabs1 = sess.query(PublishedFABS).filter_by(afa_generated_unique='unique1').first()
    pub_fabs2 = sess.query(PublishedFABS).filter_by(afa_generated_unique='unique2').first()
    expected1, expected2 = [], []
    for value in file_generation_manager.fileD2.db_columns:
        # loop through all values and format date columns
        if value in ['period_of_performance_star', 'period_of_performance_curr', 'modified_at', 'action_date']:
            expected1.append(re.sub(r"[-]", r"", str(pub_fabs1.__dict__[value]))[0:8])
            expected2.append(re.sub(r"[-]", r"", str(pub_fabs2.__dict__[value]))[0:8])
        else:
            expected1.append(str(pub_fabs1.__dict__[value] or ''))
            expected2.append(str(pub_fabs2.__dict__[value] or ''))

    assert expected1 in file_rows
    assert expected2 in file_rows


@pytest.mark.usefixtures("job_constants", "broker_files_tmp_dir")
def test_generate_funding_d2(database, monkeypatch):
    sess = database.session
    monkeypatch.setattr(file_generation_manager, 'get_timestamp', Mock(return_value='123456789'))

    pub_fabs_1 = PublishedFABSFactory(funding_agency_code='123', action_date='20170101', afa_generated_unique='unique1',
                                      is_active=True)
    pub_fabs_2 = PublishedFABSFactory(funding_agency_code='123', action_date='20170131', afa_generated_unique='unique2',
                                      is_active=True)
    pub_fabs_3 = PublishedFABSFactory(funding_agency_code='123', action_date='20161231', afa_generated_unique='unique3',
                                      is_active=True)
    pub_fabs_4 = PublishedFABSFactory(funding_agency_code='123', action_date='20170201', afa_generated_unique='unique4',
                                      is_active=True)
    pub_fabs_5 = PublishedFABSFactory(funding_agency_code='123', action_date='20170115', afa_generated_unique='unique5',
                                      is_active=False)
    pub_fabs_6 = PublishedFABSFactory(funding_agency_code='234', action_date='20170115', afa_generated_unique='unique6',
                                      is_active=True)
    file_gen = FileGenerationFactory(request_date=datetime.now().date(), start_date='01/01/2017', end_date='01/31/2017',
                                     file_type='D2', agency_code='123', agency_type='funding', is_cached_file=True,
                                     file_path=None, file_format='csv')
    sess.add_all([pub_fabs_1, pub_fabs_2, pub_fabs_3, pub_fabs_4, pub_fabs_5, pub_fabs_6, file_gen])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], file_generation=file_gen)
    file_gen_manager.generate_file()

    assert file_gen.file_path == os.path.join(CONFIG_BROKER['broker_files'],
                                              'File-D2_20170101_20170131_funding_123456789.csv')

    # check headers
    file_rows = read_file_rows(file_gen.file_path)
    assert file_rows[0] == [val[0] for key, val in file_generation_manager.fileD2.mapping.items()]

    # check body
    pub_fabs1 = sess.query(PublishedFABS).filter_by(afa_generated_unique='unique1').first()
    pub_fabs2 = sess.query(PublishedFABS).filter_by(afa_generated_unique='unique2').first()
    expected1, expected2 = [], []
    for value in file_generation_manager.fileD2.db_columns:
        # loop through all values and format date columns
        if value in ['period_of_performance_star', 'period_of_performance_curr', 'modified_at', 'action_date']:
            expected1.append(re.sub(r"[-]", r"", str(pub_fabs1.__dict__[value]))[0:8])
            expected2.append(re.sub(r"[-]", r"", str(pub_fabs2.__dict__[value]))[0:8])
        else:
            expected1.append(str(pub_fabs1.__dict__[value] or ''))
            expected2.append(str(pub_fabs2.__dict__[value] or ''))

    assert expected1 in file_rows
    assert expected2 in file_rows


@pytest.mark.usefixtures("job_constants", "broker_files_tmp_dir")
def test_generate_txt_d1(database, monkeypatch):
    sess = database.session
    monkeypatch.setattr(file_generation_manager, 'get_timestamp', Mock(return_value='123456789'))

    dap_model = DetachedAwardProcurementFactory
    dap_1 = dap_model(awarding_agency_code='123', action_date='20170101', detached_award_proc_unique='unique1')
    dap_2 = dap_model(awarding_agency_code='123', action_date='20170131', detached_award_proc_unique='unique2')
    dap_3 = dap_model(awarding_agency_code='123', action_date='20170201', detached_award_proc_unique='unique3')
    dap_4 = dap_model(awarding_agency_code='123', action_date='20161231', detached_award_proc_unique='unique4')
    dap_5 = dap_model(awarding_agency_code='234', action_date='20170115', detached_award_proc_unique='unique5')
    file_gen = FileGenerationFactory(request_date=datetime.now().date(), start_date='01/01/2017', end_date='01/31/2017',
                                     file_type='D1', agency_code='123', agency_type='awarding', is_cached_file=True,
                                     file_path=None, file_format='txt')
    sess.add_all([dap_1, dap_2, dap_3, dap_4, dap_5, file_gen])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], file_generation=file_gen)
    file_gen_manager.generate_file()

    assert file_gen.file_path == os.path.join(CONFIG_BROKER['broker_files'],
                                              'File-D1_20170101_20170131_awarding_123456789.txt')

    # check headers
    file_rows = read_file_rows(file_gen.file_path, delimiter='|')
    assert file_rows[0] == [val[0] for key, val in file_generation_manager.fileD1.mapping.items()]

    # check body
    dap_one = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique1').first()
    dap_two = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique2').first()
    expected1, expected2 = [], []
    for value in file_generation_manager.fileD1.db_columns:
        # loop through all values and format date columns
        if value in ['period_of_performance_star', 'period_of_performance_curr', 'period_of_perf_potential_e',
                     'ordering_period_end_date', 'action_date', 'last_modified', 'solicitation_date']:
            expected1.append(re.sub(r"[-]", r"", str(dap_one.__dict__[value]))[0:8])
            expected2.append(re.sub(r"[-]", r"", str(dap_two.__dict__[value]))[0:8])
        elif value in d1_booleans:
            expected1.append(str(dap_one.__dict__[value])[0:1].lower() if dap_one.__dict__[value] is not None else None)
            expected2.append(str(dap_two.__dict__[value])[0:1].lower() if dap_two.__dict__[value] is not None else None)
        else:
            expected1.append(str(dap_one.__dict__[value]))
            expected2.append(str(dap_two.__dict__[value]))

    assert expected1 in file_rows
    assert expected2 in file_rows


@pytest.mark.usefixtures("job_constants", "broker_files_tmp_dir")
def test_generate_file_updates_jobs(monkeypatch, database):
    sess = database.session
    job1 = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['award_procurement'], filename=None, original_filename=None,
                      start_date='01/01/2017', end_date='01/31/2017', submission=None)
    job2 = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['award_procurement'], filename=None, original_filename=None,
                      start_date='01/01/2017', end_date='01/31/2017', submission=None)
    job3 = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['award_procurement'], filename=None, original_filename=None,
                      start_date='01/01/2017', end_date='01/31/2017', submission=None)
    file_gen = FileGenerationFactory(request_date=datetime.now().date(), start_date='01/01/2017', end_date='01/31/2017',
                                     file_type='D1', agency_code='123', agency_type='awarding', is_cached_file=True,
                                     file_path=None, file_format='csv')
    sess.add_all([job1, job2, job3, file_gen])
    sess.commit()
    job1.file_generation_id = file_gen.file_generation_id
    job2.file_generation_id = file_gen.file_generation_id
    job3.file_generation_id = file_gen.file_generation_id
    sess.commit()

    monkeypatch.setattr(generation_helper, 'g', Mock(return_value={'is_local': CONFIG_BROKER['local']}))
    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], file_generation=file_gen)
    file_gen_manager.generate_file()
    sess.refresh(file_gen)

    original_filename = file_gen.file_path.split('/')[-1]

    assert job1.job_status_id == JOB_STATUS_DICT['finished']
    assert job1.original_filename == original_filename
    assert job1.filename == '{}{}'.format(
        CONFIG_BROKER['broker_files'] if CONFIG_BROKER['local'] else job1.submission_id + '/', original_filename)

    assert job2.job_status_id == JOB_STATUS_DICT['finished']
    assert job2.original_filename == original_filename
    assert job2.filename == '{}{}'.format(
        CONFIG_BROKER['broker_files'] if CONFIG_BROKER['local'] else job2.submission_id + '/', original_filename)

    assert job3.job_status_id == JOB_STATUS_DICT['finished']
    assert job3.original_filename == original_filename
    assert job3.filename == '{}{}'.format(
        CONFIG_BROKER['broker_files'] if CONFIG_BROKER['local'] else job3.submission_id + '/', original_filename)


@pytest.mark.usefixtures("job_constants")
def test_generate_e_file(mock_broker_config_paths, database):
    """ Verify that generate_e_file makes an appropriate query (matching both D1 and D2 entries) and creates
        a file matching the expected recipient
    """
    # Generate several file D1 entries, largely with the same submission_id, and with two overlapping UEI. Generate
    # several D2 entries with the same submission_id as well
    sess = database.session
    sub = SubmissionFactory()
    sub_2 = SubmissionFactory()

    file_path = str(mock_broker_config_paths['broker_files'].join('e_test1'))
    job = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['executive_compensation'], filename=file_path,
                     original_filename='e_test1', submission=sub)
    database.session.add_all([sub, sub_2, job])
    database.session.commit()

    model = AwardProcurementFactory(submission_id=sub.submission_id)
    aps = [AwardProcurementFactory(submission_id=sub.submission_id) for _ in range(4)]
    afas = [AwardFinancialAssistanceFactory(submission_id=sub.submission_id) for _ in range(5)]
    same_uei = AwardProcurementFactory(
        submission_id=sub.submission_id,
        awardee_or_recipient_uei=model.awardee_or_recipient_uei)
    unrelated = AwardProcurementFactory(submission_id=sub_2.submission_id)
    uei_list = [SAMRecipientFactory(uei=model.awardee_or_recipient_uei)]
    uei_list.extend([SAMRecipientFactory(uei=ap.awardee_or_recipient_uei) for ap in aps])
    uei_list.extend([SAMRecipientFactory(uei=afa.awardee_or_recipient_uei) for afa in afas])
    sess.add_all(aps + afas + uei_list + [model, same_uei, unrelated])
    sess.commit()

    file_gen_manager = FileGenerationManager(database.session, CONFIG_BROKER['local'], job=job)
    file_gen_manager.generate_file()

    # check headers
    file_rows = read_file_rows(file_path)
    assert file_rows[0] == ['AwardeeOrRecipientUEI', 'AwardeeOrRecipientLegalEntityName', 'UltimateParentUEI',
                            'UltimateParentLegalEntityName', 'HighCompOfficer1FullName', 'HighCompOfficer1Amount',
                            'HighCompOfficer2FullName', 'HighCompOfficer2Amount', 'HighCompOfficer3FullName',
                            'HighCompOfficer3Amount', 'HighCompOfficer4FullName', 'HighCompOfficer4Amount',
                            'HighCompOfficer5FullName', 'HighCompOfficer5Amount']

    # Check listed UEI
    expected = [[uei.uei, uei.legal_business_name, uei.ultimate_parent_uei, uei.ultimate_parent_legal_enti,
                 uei.high_comp_officer1_full_na, uei.high_comp_officer1_amount, uei.high_comp_officer2_full_na,
                 uei.high_comp_officer2_amount, uei.high_comp_officer3_full_na, uei.high_comp_officer3_amount,
                 uei.high_comp_officer4_full_na, uei.high_comp_officer4_amount, uei.high_comp_officer5_full_na,
                 uei.high_comp_officer5_amount]
                for uei in uei_list]
    received = [file_row for file_row in file_rows[1:]]
    assert sorted(received) == list(sorted(expected))


def read_file_rows(file_path, delimiter=','):
    """ Helper to read the file rows in the provided file. """
    assert os.path.isfile(file_path)

    with open(file_path) as f:
        return [row for row in csv.reader(f, delimiter=delimiter)]
