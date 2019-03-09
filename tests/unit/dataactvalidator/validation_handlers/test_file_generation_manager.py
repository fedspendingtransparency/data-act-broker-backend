import csv
import os
import re
import pytest

from collections import OrderedDict
from datetime import datetime
from unittest.mock import Mock

from dataactbroker.helpers import generation_helper

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.models.stagingModels import DetachedAwardProcurement, PublishedAwardFinancialAssistance
from dataactcore.models.domainModels import SF133, concat_tas_dict
from dataactcore.utils import fileE

from dataactvalidator.validation_handlers import file_generation_manager
from dataactvalidator.validation_handlers.file_generation_manager import FileGenerationManager

from tests.unit.dataactcore.factories.job import JobFactory, FileGenerationFactory, SubmissionFactory
from tests.unit.dataactcore.factories.domain import TASFactory, SF133Factory
from tests.unit.dataactcore.factories.staging import (
    AwardFinancialAssistanceFactory, AwardProcurementFactory, DetachedAwardProcurementFactory,
    PublishedAwardFinancialAssistanceFactory)


@pytest.mark.usefixtures("job_constants")
def test_generate_a(mock_broker_config_paths, database):
    sess = database.session

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
    tas1 = TASFactory(financial_indicator2=' ', **tas1_dict)
    tas2 = TASFactory(financial_indicator2=' ', **tas2_dict)
    job = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['appropriations'], filename=None, start_date='01/01/2017',
                     end_date='03/31/2017', submission_id=None)
    sess.add_all([sf1, sf2, sf3, tas1, tas2, job])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], job=job)
    # providing agency code here as it will be passed via SQS and detached file jobs don't store agency code
    file_gen_manager.generate_file(agency_cgac)

    assert job.filename is not None

    # check headers
    file_rows = read_file_rows(job.filename)
    assert file_rows[0] == [key for key in file_generation_manager.fileA.mapping]

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
    for value in file_generation_manager.fileA.db_columns:
        # loop through all values and format date columns
        if value in sf1.__dict__:
            expected1.append(str(sf1.__dict__[value] or ''))
            expected2.append(str(sf2.__dict__[value] or ''))
        elif value in expected1_sum_cols:
            expected1.append(expected1_sum_cols[value])
            expected2.append(expected2_sum_cols[value])

    assert expected1 in file_rows
    assert expected2 in file_rows


@pytest.mark.usefixtures("job_constants")
def test_generate_awarding_d1(mock_broker_config_paths, database):
    sess = database.session
    dap_model = DetachedAwardProcurementFactory
    dap_1 = dap_model(awarding_agency_code='123', action_date='20170101', detached_award_proc_unique='unique1')
    dap_2 = dap_model(awarding_agency_code='123', action_date='20170131', detached_award_proc_unique='unique2')
    dap_3 = dap_model(awarding_agency_code='123', action_date='20170201', detached_award_proc_unique='unique3')
    dap_4 = dap_model(awarding_agency_code='123', action_date='20161231', detached_award_proc_unique='unique4')
    dap_5 = dap_model(awarding_agency_code='234', action_date='20170115', detached_award_proc_unique='unique5')
    file_gen = FileGenerationFactory(request_date=datetime.now().date(), start_date='01/01/2017', end_date='01/31/2017',
                                     file_type='D1', agency_code='123', agency_type='awarding', is_cached_file=True,
                                     file_path=None)
    sess.add_all([dap_1, dap_2, dap_3, dap_4, dap_5, file_gen])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], file_generation=file_gen)
    file_gen_manager.generate_file()

    assert file_gen.file_path is not None

    # check headers
    file_rows = read_file_rows(file_gen.file_path)
    assert file_rows[0] == [key for key in file_generation_manager.fileD1.mapping]

    # check body
    dap_one = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique1').first()
    dap_two = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique2').first()
    expected1, expected2 = [], []
    for value in file_generation_manager.fileD1.db_columns:
        # loop through all values and format date columns
        if value in ['period_of_performance_star', 'period_of_performance_curr', 'period_of_perf_potential_e',
                     'ordering_period_end_date', 'action_date', 'last_modified']:
            expected1.append(re.sub(r"[-]", r"", str(dap_one.__dict__[value]))[0:8])
            expected2.append(re.sub(r"[-]", r"", str(dap_two.__dict__[value]))[0:8])
        else:
            expected1.append(str(dap_one.__dict__[value]))
            expected2.append(str(dap_two.__dict__[value]))

    assert expected1 in file_rows
    assert expected2 in file_rows


@pytest.mark.usefixtures("job_constants")
def test_generate_funding_d1(mock_broker_config_paths, database):
    sess = database.session
    dap_model = DetachedAwardProcurementFactory
    dap_1 = dap_model(funding_agency_code='123', action_date='20170101', detached_award_proc_unique='unique1')
    dap_2 = dap_model(funding_agency_code='123', action_date='20170131', detached_award_proc_unique='unique2')
    dap_3 = dap_model(funding_agency_code='123', action_date='20170201', detached_award_proc_unique='unique3')
    dap_4 = dap_model(funding_agency_code='123', action_date='20161231', detached_award_proc_unique='unique4')
    dap_5 = dap_model(funding_agency_code='234', action_date='20170115', detached_award_proc_unique='unique5')
    file_gen = FileGenerationFactory(request_date=datetime.now().date(), start_date='01/01/2017', end_date='01/31/2017',
                                     file_type='D1', agency_code='123', agency_type='funding', is_cached_file=True,
                                     file_path=None)
    sess.add_all([dap_1, dap_2, dap_3, dap_4, dap_5, file_gen])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], file_generation=file_gen)
    file_gen_manager.generate_file()

    assert file_gen.file_path is not None

    # check headers
    file_rows = read_file_rows(file_gen.file_path)
    assert file_rows[0] == [key for key in file_generation_manager.fileD1.mapping]

    # check body
    dap_one = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique1').first()
    dap_two = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique2').first()
    expected1, expected2 = [], []
    for value in file_generation_manager.fileD1.db_columns:
        # loop through all values and format date columns
        if value in ['period_of_performance_star', 'period_of_performance_curr', 'period_of_perf_potential_e',
                     'ordering_period_end_date', 'action_date', 'last_modified']:
            expected1.append(re.sub(r"[-]", r"", str(dap_one.__dict__[value]))[0:8])
            expected2.append(re.sub(r"[-]", r"", str(dap_two.__dict__[value]))[0:8])
        else:
            expected1.append(str(dap_one.__dict__[value]))
            expected2.append(str(dap_two.__dict__[value]))

    assert expected1 in file_rows
    assert expected2 in file_rows


@pytest.mark.usefixtures("job_constants")
def test_generate_awarding_d2(mock_broker_config_paths, database):
    sess = database.session
    pafa = PublishedAwardFinancialAssistanceFactory
    pafa_1 = pafa(awarding_agency_code='123', action_date='20170101', afa_generated_unique='unique1', is_active=True)
    pafa_2 = pafa(awarding_agency_code='123', action_date='20170131', afa_generated_unique='unique2', is_active=True)
    pafa_3 = pafa(awarding_agency_code='123', action_date='20161231', afa_generated_unique='unique3', is_active=True)
    pafa_4 = pafa(awarding_agency_code='123', action_date='20170201', afa_generated_unique='unique4', is_active=True)
    pafa_5 = pafa(awarding_agency_code='123', action_date='20170115', afa_generated_unique='unique5', is_active=False)
    pafa_6 = pafa(awarding_agency_code='234', action_date='20170115', afa_generated_unique='unique6', is_active=True)
    file_gen = FileGenerationFactory(request_date=datetime.now().date(), start_date='01/01/2017', end_date='01/31/2017',
                                     file_type='D2', agency_code='123', agency_type='awarding', is_cached_file=True,
                                     file_path=None)
    sess.add_all([pafa_1, pafa_2, pafa_3, pafa_4, pafa_5, pafa_6, file_gen])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], file_generation=file_gen)
    file_gen_manager.generate_file()

    assert file_gen.file_path is not None

    # check headers
    file_rows = read_file_rows(file_gen.file_path)
    assert file_rows[0] == [key for key in file_generation_manager.fileD2.mapping]

    # check body
    pafa1 = sess.query(PublishedAwardFinancialAssistance).filter_by(afa_generated_unique='unique1').first()
    pafa2 = sess.query(PublishedAwardFinancialAssistance).filter_by(afa_generated_unique='unique2').first()
    expected1, expected2 = [], []
    for value in file_generation_manager.fileD2.db_columns:
        # loop through all values and format date columns
        if value in ['period_of_performance_star', 'period_of_performance_curr', 'modified_at', 'action_date']:
            expected1.append(re.sub(r"[-]", r"", str(pafa1.__dict__[value]))[0:8])
            expected2.append(re.sub(r"[-]", r"", str(pafa2.__dict__[value]))[0:8])
        else:
            expected1.append(str(pafa1.__dict__[value]))
            expected2.append(str(pafa2.__dict__[value]))

    assert expected1 in file_rows
    assert expected2 in file_rows


@pytest.mark.usefixtures("job_constants")
def test_generate_funding_d2(mock_broker_config_paths, database):
    sess = database.session
    pafa = PublishedAwardFinancialAssistanceFactory
    pafa_1 = pafa(funding_agency_code='123', action_date='20170101', afa_generated_unique='unique1', is_active=True)
    pafa_2 = pafa(funding_agency_code='123', action_date='20170131', afa_generated_unique='unique2', is_active=True)
    pafa_3 = pafa(funding_agency_code='123', action_date='20161231', afa_generated_unique='unique3', is_active=True)
    pafa_4 = pafa(funding_agency_code='123', action_date='20170201', afa_generated_unique='unique4', is_active=True)
    pafa_5 = pafa(funding_agency_code='123', action_date='20170115', afa_generated_unique='unique5', is_active=False)
    pafa_6 = pafa(funding_agency_code='234', action_date='20170115', afa_generated_unique='unique6', is_active=True)
    file_gen = FileGenerationFactory(request_date=datetime.now().date(), start_date='01/01/2017', end_date='01/31/2017',
                                     file_type='D2', agency_code='123', agency_type='funding', is_cached_file=True,
                                     file_path=None)
    sess.add_all([pafa_1, pafa_2, pafa_3, pafa_4, pafa_5, pafa_6, file_gen])
    sess.commit()

    file_gen_manager = FileGenerationManager(sess, CONFIG_BROKER['local'], file_generation=file_gen)
    file_gen_manager.generate_file()

    assert file_gen.file_path is not None

    # check headers
    file_rows = read_file_rows(file_gen.file_path)
    assert file_rows[0] == [key for key in file_generation_manager.fileD2.mapping]

    # check body
    pafa1 = sess.query(PublishedAwardFinancialAssistance).filter_by(afa_generated_unique='unique1').first()
    pafa2 = sess.query(PublishedAwardFinancialAssistance).filter_by(afa_generated_unique='unique2').first()
    expected1, expected2 = [], []
    for value in file_generation_manager.fileD2.db_columns:
        # loop through all values and format date columns
        if value in ['period_of_performance_star', 'period_of_performance_curr', 'modified_at', 'action_date']:
            expected1.append(re.sub(r"[-]", r"", str(pafa1.__dict__[value]))[0:8])
            expected2.append(re.sub(r"[-]", r"", str(pafa2.__dict__[value]))[0:8])
        else:
            expected1.append(str(pafa1.__dict__[value]))
            expected2.append(str(pafa2.__dict__[value]))

    assert expected1 in file_rows
    assert expected2 in file_rows


@pytest.mark.usefixtures("job_constants")
def test_generate_file_updates_jobs(monkeypatch, mock_broker_config_paths, database):
    sess = database.session
    job1 = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['award_procurement'], filename=None, original_filename=None,
                      start_date='01/01/2017', end_date='01/31/2017')
    job2 = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['award_procurement'], filename=None, original_filename=None,
                      start_date='01/01/2017', end_date='01/31/2017')
    job3 = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['award_procurement'], filename=None, original_filename=None,
                      start_date='01/01/2017', end_date='01/31/2017')
    file_gen = FileGenerationFactory(request_date=datetime.now().date(), start_date='01/01/2017',
                                     end_date='01/31/2017', file_type='D1', agency_code='123',
                                     agency_type='awarding', is_cached_file=True, file_path=None)
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
def test_generate_e_file_query(monkeypatch, mock_broker_config_paths, database):
    """ Verify that generate_e_file makes an appropriate query (matching both D1 and D2 entries) """
    # Generate several file D1 entries, largely with the same submission_id, and with two overlapping DUNS. Generate
    # several D2 entries with the same submission_id as well
    sess = database.session
    sub = SubmissionFactory()
    sub_2 = SubmissionFactory()
    sess.add_all([sub, sub_2])
    sess.commit()

    file_path = str(mock_broker_config_paths['broker_files'].join('e_test1'))
    job = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['executive_compensation'], filename=file_path,
                     original_filename='e_test1', submission_id=sub.submission_id)
    database.session.add(job)
    database.session.commit()

    model = AwardProcurementFactory(submission_id=sub.submission_id)
    aps = [AwardProcurementFactory(submission_id=sub.submission_id) for _ in range(4)]
    afas = [AwardFinancialAssistanceFactory(submission_id=sub.submission_id) for _ in range(5)]
    same_duns = AwardProcurementFactory(
        submission_id=sub.submission_id,
        awardee_or_recipient_uniqu=model.awardee_or_recipient_uniqu)
    unrelated = AwardProcurementFactory(submission_id=sub_2.submission_id)
    sess.add_all(aps + afas + [model, same_duns, unrelated])
    sess.commit()

    monkeypatch.setattr(file_generation_manager.fileE, 'retrieve_rows', Mock(return_value=[]))

    file_gen_manager = FileGenerationManager(database.session, CONFIG_BROKER['local'], job=job)
    file_gen_manager.generate_file()

    # [0][0] gives us the first, non-keyword args
    call_args = file_generation_manager.fileE.retrieve_rows.call_args[0][0]
    expected = [ap.awardee_or_recipient_uniqu for ap in aps]
    expected.append(model.awardee_or_recipient_uniqu)
    expected.extend(afa.awardee_or_recipient_uniqu for afa in afas)
    assert list(sorted(call_args)) == list(sorted(expected))


@pytest.mark.usefixtures("job_constants")
def test_generate_e_file_csv(monkeypatch, mock_broker_config_paths, database):
    """ Verify that an appropriate CSV is written, based on fileE.Row's structure """
    # Create an award so that we have _a_ duns
    sub = SubmissionFactory()
    database.session.add(sub)
    database.session.commit()

    ap = AwardProcurementFactory(submission_id=sub.submission_id)
    database.session.add(ap)
    database.session.commit()

    file_path = str(mock_broker_config_paths['broker_files'].join('e_test1'))
    job = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['executive_compensation'], filename=file_path,
                     original_filename='e_test1', submission_id=sub.submission_id)
    database.session.add(job)
    database.session.commit()

    monkeypatch.setattr(file_generation_manager.fileE, 'row_to_dict', Mock())
    file_generation_manager.fileE.row_to_dict.return_value = {}

    monkeypatch.setattr(file_generation_manager.fileE, 'retrieve_rows', Mock())
    file_generation_manager.fileE.retrieve_rows.return_value = [
        fileE.Row('a', 'b', 'c', 'd', '1a', '1b', '2a', '2b', '3a', '3b', '4a', '4b', '5a', '5b'),
        fileE.Row('A', 'B', 'C', 'D', '1A', '1B', '2A', '2B', '3A', '3B', '4A', '4B', '5A', '5B')
    ]

    file_gen_manager = FileGenerationManager(database.session, CONFIG_BROKER['local'], job=job)
    file_gen_manager.generate_file()

    expected = [
        ['AwardeeOrRecipientUniqueIdentifier', 'AwardeeOrRecipientLegalEntityName', 'UltimateParentUniqueIdentifier',
         'UltimateParentLegalEntityName', 'HighCompOfficer1FullName', 'HighCompOfficer1Amount',
         'HighCompOfficer2FullName', 'HighCompOfficer2Amount', 'HighCompOfficer3FullName', 'HighCompOfficer3Amount',
         'HighCompOfficer4FullName', 'HighCompOfficer4Amount', 'HighCompOfficer5FullName', 'HighCompOfficer5Amount'],
        ['a', 'b', 'c', 'd', '1a', '1b', '2a', '2b', '3a', '3b', '4a', '4b', '5a', '5b'],
        ['A', 'B', 'C', 'D', '1A', '1B', '2A', '2B', '3A', '3B', '4A', '4B', '5A', '5B']
    ]
    assert read_file_rows(file_path) == expected


def read_file_rows(file_path):
    assert os.path.isfile(file_path)

    with open(file_path) as f:
        return [row for row in csv.reader(f)]
