import csv
import os
import re

from collections import OrderedDict
from unittest.mock import Mock

from dataactbroker.handlers import fileGenerationHandler
from dataactcore.models.jobModels import FileType, JobStatus, JobType
from dataactcore.models.stagingModels import DetachedAwardProcurement, PublishedAwardFinancialAssistance
from dataactcore.utils import fileE
from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory, FileRequestFactory
from tests.unit.dataactcore.factories.staging import (
    AwardFinancialAssistanceFactory, AwardProcurementFactory, DetachedAwardProcurementFactory,
    PublishedAwardFinancialAssistanceFactory)


def read_file_rows(file_path):
    assert os.path.isfile(file_path)

    with open(file_path) as f:
        return [row for row in csv.reader(f)]


def test_generate_d1_file_query(monkeypatch, mock_broker_config_paths, database, job_constants):
    """A CSV with fields in the right order should be written to the file system"""
    sess = database.session
    dap_model = DetachedAwardProcurementFactory
    dap_1 = dap_model(awarding_agency_code='123', action_date='20170101', detached_award_proc_unique='unique1')
    dap_2 = dap_model(awarding_agency_code='123', action_date='20170131', detached_award_proc_unique='unique2')
    dap_3 = dap_model(awarding_agency_code='123', action_date='20170201', detached_award_proc_unique='unique3')
    dap_4 = dap_model(awarding_agency_code='123', action_date='20161231', detached_award_proc_unique='unique4')
    dap_5 = dap_model(awarding_agency_code='234', action_date='20170115', detached_award_proc_unique='unique5')
    sess.add_all([dap_1, dap_2, dap_3, dap_4, dap_5])

    job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='running').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award_procurement').one(),
    )
    sess.add(job)
    sess.commit()

    file_path = str(mock_broker_config_paths['d_file_storage_path'].join('d1'))
    fileGenerationHandler.generate_d_file('D1', '123', '01/01/2017', '01/31/2017', job.job_id, 'd1', is_local=True)

    # check headers
    file_rows = read_file_rows(file_path)
    assert file_rows[0] == [key for key in fileGenerationHandler.fileD1.mapping]

    # check body
    dap_one = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique1').first()
    dap_two = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique2').first()
    expected1, expected2 = [], []
    for value in fileGenerationHandler.fileD1.db_columns:
        # loop through all values and format date columns
        if value in ['period_of_performance_star', 'period_of_performance_curr', 'period_of_perf_potential_e',
                     'ordering_period_end_date', 'action_date', 'last_modified']:
            expected1.append(re.sub(r"[-]", r"", str(dap_one.__dict__[value]))[0:8])
            expected2.append(re.sub(r"[-]", r"", str(dap_two.__dict__[value]))[0:8])
        else:
            expected1.append(str(dap_one.__dict__[value]))
            expected2.append(str(dap_two.__dict__[value]))

    print(file_rows[1])
    print("===============================================================")
    print(expected1)
    print("===============================================================")
    print(file_rows[2])
    print("===============================================================")
    print(expected1)

    assert file_rows[1] == expected1 or file_rows[2] == expected1
    assert file_rows[1] == expected2 or file_rows[2] == expected2


def test_generate_d2_file_query(monkeypatch, mock_broker_config_paths, database, job_constants):
    """A CSV with fields in the right order should be written to the file system"""
    sess = database.session
    pafa = PublishedAwardFinancialAssistanceFactory
    pafa_1 = pafa(awarding_agency_code='123', action_date='20170101', afa_generated_unique='unique1', is_active=True)
    pafa_2 = pafa(awarding_agency_code='123', action_date='20170131', afa_generated_unique='unique2', is_active=True)
    pafa_3 = pafa(awarding_agency_code='123', action_date='20161231', afa_generated_unique='unique3', is_active=True)
    pafa_4 = pafa(awarding_agency_code='123', action_date='20170201', afa_generated_unique='unique4', is_active=True)
    pafa_5 = pafa(awarding_agency_code='123', action_date='20170115', afa_generated_unique='unique5', is_active=False)
    pafa_6 = pafa(awarding_agency_code='234', action_date='20170115', afa_generated_unique='unique6', is_active=True)
    sess.add_all([pafa_1, pafa_2, pafa_3, pafa_4, pafa_5, pafa_6])

    job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='running').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award').one(),
    )
    sess.add(job)
    sess.commit()

    file_path = str(mock_broker_config_paths['d_file_storage_path'].join('d2'))
    fileGenerationHandler.generate_d_file('D2', '123', '01/01/2017', '01/31/2017', job.job_id, 'd2', is_local=True)

    # check headers
    file_rows = read_file_rows(file_path)
    assert file_rows[0] == [key for key in fileGenerationHandler.fileD2.mapping]

    # check body
    pafa1 = sess.query(PublishedAwardFinancialAssistance).filter_by(afa_generated_unique='unique1').first()
    pafa2 = sess.query(PublishedAwardFinancialAssistance).filter_by(afa_generated_unique='unique2').first()
    expected1, expected2 = [], []
    for value in fileGenerationHandler.fileD2.db_columns:
        # loop through all values and format date columns
        if value in ['period_of_performance_star', 'period_of_performance_curr', 'modified_at', 'action_date']:
            expected1.append(re.sub(r"[-]", r"", str(pafa1.__dict__[value]))[0:8])
            expected2.append(re.sub(r"[-]", r"", str(pafa2.__dict__[value]))[0:8])
        else:
            expected1.append(str(pafa1.__dict__[value]))
            expected2.append(str(pafa2.__dict__[value]))

    assert file_rows[1] == expected1 or file_rows[2] == expected1
    assert file_rows[1] == expected2 or file_rows[2] == expected2


def test_generate_d_file_success_regen(monkeypatch, mock_broker_config_paths, database, job_constants):
    """Testing that a new file is not generated if this job already has a successfully generated file"""
    sess = database.session
    job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='running').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award').one(),
        filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
        original_filename='original', from_cached=True,
    )
    sess.add(job)
    sess.commit()

    file_request = FileRequestFactory(
        job=job, is_cached_file=True, agency_code='123', start_date='01/01/2017', end_date='01/31/2017',
        file_type='d2',)
    sess.add(file_request)
    sess.commit()

    monkeypatch.setattr(fileGenerationHandler, 'write_query_to_file', Mock(return_value=None))
    fileGenerationHandler.generate_d_file('D2', '123', '01/01/2017', '01/31/2017', job.job_id, 'not_original',
                                          is_local=True, old_filename='original')
    sess.refresh(job)
    assert job.original_filename == 'original'
    assert job.job_status_id == sess.query(JobStatus).filter_by(name='finished').one().job_status_id


def test_generate_d_file_success_new_gen(monkeypatch, mock_broker_config_paths, database, job_constants):
    """Testing that a new file is not generated if another job has already has a successfully generated file"""
    sess = database.session
    original_job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='finished').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award').one(),
        filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
        original_filename='original', from_cached=True,
    )
    sess.add(original_job)
    sess.commit()

    file_request = FileRequestFactory(
        job=original_job, is_cached_file=True, agency_code='123', start_date='01/01/2017', end_date='01/31/2017',
        file_type='D2',)
    new_job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='running').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award').one(),
    )
    sess.add_all([file_request, new_job])
    sess.commit()

    monkeypatch.setattr(fileGenerationHandler, 'write_query_to_file', Mock(return_value=None))
    fileGenerationHandler.generate_d_file('D2', '123', '01/01/2017', '01/31/2017', new_job.job_id, 'not_original',
                                          is_local=True)
    sess.refresh(new_job)
    assert new_job.original_filename == 'original'
    assert new_job.job_status_id == sess.query(JobStatus).filter_by(name='finished').one().job_status_id


def test_generate_f_file(monkeypatch, mock_broker_config_paths):
    """A CSV with fields in the right order should be written to the file system"""
    file_f_mock = Mock()
    monkeypatch.setattr(fileGenerationHandler, 'fileF', file_f_mock)
    file_f_mock.generate_f_rows.return_value = [dict(key4='a', key11='b'), dict(key4='c', key11='d')]

    file_f_mock.mappings = OrderedDict([('key4', 'mapping4'), ('key11', 'mapping11')])
    file_path = str(mock_broker_config_paths['broker_files'].join('uniq1'))
    expected = [['key4', 'key11'], ['a', 'b'], ['c', 'd']]

    monkeypatch.setattr(fileGenerationHandler, 'mark_job_status', Mock())
    fileGenerationHandler.generate_f_file(1, 1, 'uniq1', 'uniq1', is_local=True)
    assert read_file_rows(file_path) == expected

    # re-order
    file_f_mock.mappings = OrderedDict([('key11', 'mapping11'), ('key4', 'mapping4')])
    file_path = str(mock_broker_config_paths['broker_files'].join('uniq2'))
    expected = [['key11', 'key4'], ['b', 'a'], ['d', 'c']]

    monkeypatch.setattr(fileGenerationHandler, 'mark_job_status', Mock())
    fileGenerationHandler.generate_f_file(1, 1, 'uniq2', 'uniq2', is_local=True)
    assert read_file_rows(file_path) == expected


def test_generate_e_file_query(monkeypatch, database):
    """Verify that generate_e_file makes an appropriate query (matching both D1 and D2 entries)"""
    # Generate several file D1 entries, largely with the same submission_id, and with two overlapping DUNS. Generate
    # several D2 entries with the same submission_id as well
    sess = database.session
    sub = SubmissionFactory()
    sub_2 = SubmissionFactory()
    sess.add_all([sub, sub_2])
    sess.commit()

    model = AwardProcurementFactory(submission_id=sub.submission_id)
    aps = [AwardProcurementFactory(submission_id=sub.submission_id) for _ in range(4)]
    afas = [AwardFinancialAssistanceFactory(submission_id=sub.submission_id) for _ in range(5)]
    same_duns = AwardProcurementFactory(
        submission_id=sub.submission_id,
        awardee_or_recipient_uniqu=model.awardee_or_recipient_uniqu)
    unrelated = AwardProcurementFactory(submission_id=sub_2.submission_id)
    sess.add_all(aps + afas + [model, same_duns, unrelated])
    sess.commit()

    monkeypatch.setattr(fileGenerationHandler, 'mark_job_status', Mock())
    monkeypatch.setattr(fileGenerationHandler.fileE, 'retrieve_rows', Mock(return_value=[]))

    fileGenerationHandler.generate_e_file(sub.submission_id, 1, 'uniq', 'uniq', is_local=True)

    # [0][0] gives us the first, non-keyword args
    call_args = fileGenerationHandler.fileE.retrieve_rows.call_args[0][0]
    expected = [ap.awardee_or_recipient_uniqu for ap in aps]
    expected.append(model.awardee_or_recipient_uniqu)
    expected.extend(afa.awardee_or_recipient_uniqu for afa in afas)
    assert list(sorted(call_args)) == list(sorted(expected))


def test_generate_e_file_csv(monkeypatch, mock_broker_config_paths, database):
    """Verify that an appropriate CSV is written, based on fileE.Row's structure"""
    # Create an award so that we have _a_ duns
    sess = database.session
    sub = SubmissionFactory()
    sess.add(sub)
    sess.commit()

    ap = AwardProcurementFactory(submission_id=sub.submission_id)
    sess.add(ap)
    sess.commit()

    monkeypatch.setattr(fileGenerationHandler.fileE, 'row_to_dict', Mock())
    fileGenerationHandler.fileE.row_to_dict.return_value = {}

    monkeypatch.setattr(fileGenerationHandler.fileE, 'retrieve_rows', Mock())
    fileGenerationHandler.fileE.retrieve_rows.return_value = [
        fileE.Row('a', 'b', 'c', 'd', '1a', '1b', '2a', '2b', '3a', '3b', '4a', '4b', '5a', '5b'),
        fileE.Row('A', 'B', 'C', 'D', '1A', '1B', '2A', '2B', '3A', '3B', '4A', '4B', '5A', '5B')
    ]

    monkeypatch.setattr(fileGenerationHandler, 'mark_job_status', Mock())
    fileGenerationHandler.generate_e_file(ap.submission_id, 1, 'uniq', 'uniq', is_local=True)

    file_path = str(mock_broker_config_paths['broker_files'].join('uniq'))
    expected = [
        ['AwardeeOrRecipientUniqueIdentifier',
         'AwardeeOrRecipientLegalEntityName',
         'UltimateParentUniqueIdentifier',
         'UltimateParentLegalEntityName',
         'HighCompOfficer1FullName', 'HighCompOfficer1Amount',
         'HighCompOfficer2FullName', 'HighCompOfficer2Amount',
         'HighCompOfficer3FullName', 'HighCompOfficer3Amount',
         'HighCompOfficer4FullName', 'HighCompOfficer4Amount',
         'HighCompOfficer5FullName', 'HighCompOfficer5Amount'],
        ['a', 'b', 'c', 'd', '1a', '1b', '2a', '2b', '3a', '3b', '4a', '4b', '5a', '5b'],
        ['A', 'B', 'C', 'D', '1A', '1B', '2A', '2B', '3A', '3B', '4A', '4B', '5A', '5B']
    ]
    assert read_file_rows(file_path) == expected


def test_job_context_success(database, job_constants):
    """When a job successfully runs, it should be marked as "finished" """
    sess = database.session
    job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='running').one(),
        job_type=sess.query(JobType).filter_by(name='validation').one(),
        file_type=sess.query(FileType).filter_by(name='sub_award').one(),
    )
    sess.add(job)
    sess.commit()

    with fileGenerationHandler.job_context(job.job_id, is_local=True):
        pass    # i.e. be successful

    sess.refresh(job)
    assert job.job_status.name == 'finished'


def test_job_context_fail(database, job_constants):
    """When a job raises an exception and has no retries left, it should be marked as failed"""
    sess = database.session
    job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='running').one(),
        job_type=sess.query(JobType).filter_by(name='validation').one(),
        file_type=sess.query(FileType).filter_by(name='sub_award').one(),
    )
    sess.add(job)
    sess.commit()

    with fileGenerationHandler.job_context(job.job_id, is_local=True):
        raise Exception('This failed!')

    sess.refresh(job)
    assert job.job_status.name == 'failed'
    assert job.error_message == 'This failed!'


def test_copy_parent_file_request_data(database, job_constants):
    sess = database.session

    job_one = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='finished').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award').one(),
    )
    job_two = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='running').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award').one(),
        filename='testing/the/path/new_filename'
    )
    sess.add_all([job_one, job_two])
    sess.commit()

    fileGenerationHandler.copy_parent_file_request_data(sess, job_two, job_one, 'D2', True)
    sess.refresh(job_one)
    sess.refresh(job_two)

    assert job_two.job_status_id == job_one.job_status_id
    assert job_two.filename == 'testing/the/path/{}'.format(job_one.original_filename)
    assert job_two.original_filename == job_one.original_filename
    assert job_two.number_of_errors == job_one.number_of_errors
    assert job_two.number_of_warnings == job_one.number_of_warnings
    assert job_two.from_cached is True
