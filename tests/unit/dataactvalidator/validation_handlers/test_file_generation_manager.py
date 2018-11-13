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
from dataactcore.utils import fileE

from dataactvalidator.validation_handlers import file_generation_manager
from dataactvalidator.validation_handlers.file_generation_manager import FileGenerationManager

from tests.unit.dataactcore.factories.job import JobFactory, FileGenerationFactory, SubmissionFactory
from tests.unit.dataactcore.factories.staging import (
    AwardFinancialAssistanceFactory, AwardProcurementFactory, DetachedAwardProcurementFactory,
    PublishedAwardFinancialAssistanceFactory)


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
def test_generate_f_file(monkeypatch, mock_broker_config_paths, database):
    """ A CSV with fields in the right order should be written to the file system """
    file_path1 = str(mock_broker_config_paths['broker_files'].join('f_test1'))
    job1 = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['sub_award'], filename=file_path1, original_filename='f_test1')
    file_path2 = str(mock_broker_config_paths['broker_files'].join('f_test2'))
    job2 = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['sub_award'], filename=file_path2, original_filename='f_test2')
    database.session.add_all([job1, job2])
    database.session.commit()

    file_f_mock = Mock()
    monkeypatch.setattr(file_generation_manager, 'fileF', file_f_mock)
    file_f_mock.generate_f_rows.return_value = [dict(key4='a', key11='b'), dict(key4='c', key11='d')]
    file_f_mock.mappings = OrderedDict([('key4', 'mapping4'), ('key11', 'mapping11')])
    expected = [['key4', 'key11'], ['a', 'b'], ['c', 'd']]

    manager1 = FileGenerationManager(database.session, CONFIG_BROKER['local'], job=job1)
    manager1.generate_f_file()

    assert read_file_rows(file_path1) == expected

    # re-order
    file_f_mock.mappings = OrderedDict([('key11', 'mapping11'), ('key4', 'mapping4')])
    expected = [['key11', 'key4'], ['b', 'a'], ['d', 'c']]

    manager2 = FileGenerationManager(database.session, CONFIG_BROKER['local'], job=job2)
    manager2.generate_f_file()

    assert read_file_rows(file_path2) == expected


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
