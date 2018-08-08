import csv
import os
import re
import pytest

from collections import OrderedDict
from flask import Flask
from unittest.mock import Mock

from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.models.stagingModels import DetachedAwardProcurement, PublishedAwardFinancialAssistance
from dataactcore.utils import fileE
from dataactvalidator.validation_handlers import file_generation_handler

from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory
from tests.unit.dataactcore.factories.staging import (
    AwardFinancialAssistanceFactory, AwardProcurementFactory, DetachedAwardProcurementFactory,
    PublishedAwardFinancialAssistanceFactory)


@pytest.mark.usefixtures("job_constants")
def test_job_context_success(database):
    """ When a job successfully runs, it should be marked as "finished" """
    sess = database.session
    job = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['validation'],
                     file_type_id=FILE_TYPE_DICT['sub_award'])
    sess.add(job)
    sess.commit()

    with file_generation_handler.job_context(job.job_id, is_local=True):
        pass    # i.e. be successful

    sess.refresh(job)
    assert job.job_status.name == 'finished'


@pytest.mark.usefixtures("job_constants")
def test_job_context_fail(database):
    """ When a job raises an exception and has no retries left, it should be marked as failed """
    sess = database.session
    job = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['validation'],
                     file_type_id=FILE_TYPE_DICT['sub_award'], error_message=None)
    sess.add(job)
    sess.commit()

    with file_generation_handler.job_context(job.job_id, is_local=True):
        raise Exception('This failed!')

    sess.refresh(job)
    assert job.job_status.name == 'failed'
    assert job.error_message == 'This failed!'


def read_file_rows(file_path):
    assert os.path.isfile(file_path)

    with open(file_path) as f:
        return [row for row in csv.reader(f)]


@pytest.mark.usefixtures("job_constants")
def test_generate_d1_file_query(mock_broker_config_paths, database):
    """ A CSV with fields in the right order should be written to the file system """
    sess = database.session
    dap_model = DetachedAwardProcurementFactory
    dap_1 = dap_model(awarding_agency_code='123', action_date='20170101', detached_award_proc_unique='unique1')
    dap_2 = dap_model(awarding_agency_code='123', action_date='20170131', detached_award_proc_unique='unique2')
    dap_3 = dap_model(awarding_agency_code='123', action_date='20170201', detached_award_proc_unique='unique3')
    dap_4 = dap_model(awarding_agency_code='123', action_date='20161231', detached_award_proc_unique='unique4')
    dap_5 = dap_model(awarding_agency_code='234', action_date='20170115', detached_award_proc_unique='unique5')
    sess.add_all([dap_1, dap_2, dap_3, dap_4, dap_5])

    file_path = str(mock_broker_config_paths['d_file_storage_path'].join('d1_test'))
    job = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award_procurement'], filename=file_path, original_filename='d1_test',
                     start_date='01/01/2017', end_date='01/31/2017')
    sess.add(job)
    sess.commit()

    with Flask(__name__).app_context():
        file_generation_handler.generate_d_file(database.session, job, '123', is_local=True)

    # check headers
    file_rows = read_file_rows(file_path)
    assert file_rows[0] == [key for key in file_generation_handler.fileD1.mapping]

    # check body
    dap_one = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique1').first()
    dap_two = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique='unique2').first()
    expected1, expected2 = [], []
    for value in file_generation_handler.fileD1.db_columns:
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
def test_generate_d2_file_query(mock_broker_config_paths, database):
    """ A CSV with fields in the right order should be written to the file system """
    sess = database.session
    pafa = PublishedAwardFinancialAssistanceFactory
    pafa_1 = pafa(awarding_agency_code='123', action_date='20170101', afa_generated_unique='unique1', is_active=True)
    pafa_2 = pafa(awarding_agency_code='123', action_date='20170131', afa_generated_unique='unique2', is_active=True)
    pafa_3 = pafa(awarding_agency_code='123', action_date='20161231', afa_generated_unique='unique3', is_active=True)
    pafa_4 = pafa(awarding_agency_code='123', action_date='20170201', afa_generated_unique='unique4', is_active=True)
    pafa_5 = pafa(awarding_agency_code='123', action_date='20170115', afa_generated_unique='unique5', is_active=False)
    pafa_6 = pafa(awarding_agency_code='234', action_date='20170115', afa_generated_unique='unique6', is_active=True)
    sess.add_all([pafa_1, pafa_2, pafa_3, pafa_4, pafa_5, pafa_6])

    file_path = str(mock_broker_config_paths['d_file_storage_path'].join('d2_test'))
    job = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award'], filename=file_path, original_filename='d2_test',
                     start_date='01/01/2017', end_date='01/31/2017')
    sess.add(job)
    sess.commit()

    with Flask(__name__).app_context():
        file_generation_handler.generate_d_file(database.session, job, '123', is_local=True)

    # check headers
    file_rows = read_file_rows(file_path)
    assert file_rows[0] == [key for key in file_generation_handler.fileD2.mapping]

    # check body
    pafa1 = sess.query(PublishedAwardFinancialAssistance).filter_by(afa_generated_unique='unique1').first()
    pafa2 = sess.query(PublishedAwardFinancialAssistance).filter_by(afa_generated_unique='unique2').first()
    expected1, expected2 = [], []
    for value in file_generation_handler.fileD2.db_columns:
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
def test_generate_f_file(monkeypatch, mock_broker_config_paths, database):
    """ A CSV with fields in the right order should be written to the file system """
    file_path1 = str(mock_broker_config_paths['broker_files'].join('f_test1'))
    job1 = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['sub_award'], filename=file_path1, original_filename='f_test1')
    file_path2 = str(mock_broker_config_paths['broker_files'].join('f_test2'))
    job2 = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['sub_award'], filename=file_path2, original_filename='f_test2')
    database.session.add(job1, job2)
    database.session.commit()

    file_f_mock = Mock()
    monkeypatch.setattr(file_generation_handler, 'fileF', file_f_mock)
    file_f_mock.generate_f_rows.return_value = [dict(key4='a', key11='b'), dict(key4='c', key11='d')]
    file_f_mock.mappings = OrderedDict([('key4', 'mapping4'), ('key11', 'mapping11')])
    expected = [['key4', 'key11'], ['a', 'b'], ['c', 'd']]

    monkeypatch.setattr(file_generation_handler, 'mark_job_status', Mock())

    with Flask(__name__).app_context():
        file_generation_handler.generate_f_file(database.session, job1, is_local=True)

    assert read_file_rows(file_path1) == expected

    # re-order
    file_f_mock.mappings = OrderedDict([('key11', 'mapping11'), ('key4', 'mapping4')])
    expected = [['key11', 'key4'], ['b', 'a'], ['d', 'c']]

    monkeypatch.setattr(file_generation_handler, 'mark_job_status', Mock())

    with Flask(__name__).app_context():
        file_generation_handler.generate_f_file(database.session, job2, is_local=True)

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

    monkeypatch.setattr(file_generation_handler, 'mark_job_status', Mock())
    monkeypatch.setattr(file_generation_handler.fileE, 'retrieve_rows', Mock(return_value=[]))

    with Flask(__name__).app_context():
        file_generation_handler.generate_e_file(database.session, job, is_local=True)

    # [0][0] gives us the first, non-keyword args
    call_args = file_generation_handler.fileE.retrieve_rows.call_args[0][0]
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

    monkeypatch.setattr(file_generation_handler.fileE, 'row_to_dict', Mock())
    file_generation_handler.fileE.row_to_dict.return_value = {}

    monkeypatch.setattr(file_generation_handler.fileE, 'retrieve_rows', Mock())
    file_generation_handler.fileE.retrieve_rows.return_value = [
        fileE.Row('a', 'b', 'c', 'd', '1a', '1b', '2a', '2b', '3a', '3b', '4a', '4b', '5a', '5b'),
        fileE.Row('A', 'B', 'C', 'D', '1A', '1B', '2A', '2B', '3A', '3B', '4A', '4B', '5A', '5B')
    ]

    monkeypatch.setattr(file_generation_handler, 'mark_job_status', Mock())

    with Flask(__name__).app_context():
        file_generation_handler.generate_e_file(database.session, job, is_local=True)

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


@pytest.mark.usefixtures("job_constants")
def test_copy_parent_file_request_data(database):
    sess = database.session

    job_one = JobFactory(job_status_id=JOB_STATUS_DICT['finished'], job_type_id=JOB_TYPE_DICT['file_upload'],
                         file_type_id=FILE_TYPE_DICT['award'])
    job_two = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                         file_type_id=FILE_TYPE_DICT['award'], filename='job_id/new_filename')
    sess.add_all([job_one, job_two])
    sess.commit()

    file_generation_handler.copy_parent_file_request_data(sess, job_two, job_one, True)
    sess.refresh(job_one)
    sess.refresh(job_two)

    assert job_two.job_status_id == job_one.job_status_id
    assert job_two.filename == 'job_id/{}'.format(job_one.original_filename)
    assert job_two.original_filename == job_one.original_filename
    assert job_two.number_of_errors == job_one.number_of_errors
    assert job_two.number_of_warnings == job_one.number_of_warnings
    assert job_two.from_cached is True


@pytest.mark.usefixtures("job_constants")
def test_check_detached_d_file_generation(database):
    """ Job statuses should return the correct status and error message to the user """
    sess = database.session

    # Detached D2 generation waiting to be picked up by the Validator
    job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award'], error_message='', filename='job_id/file.csv',
                     original_filename='file.csv')
    sess.add(job)
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # Detached D2 generation running in the Validator
    job.job_status_id = JOB_STATUS_DICT['running']
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # Detached D2 generation completed by the Validator
    job.job_status_id = JOB_STATUS_DICT['finished']
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'finished'
    assert response_dict['message'] == ''

    # Detached D2 generation with an unknown error
    job.job_status_id = JOB_STATUS_DICT['failed']
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Upload job failed without error message'

    # Detached D2 generation with a known error
    job.error_message = 'FABS upload error message'
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'FABS upload error message'


@pytest.mark.usefixtures("job_constants")
def test_check_submission_d_file_generation(database):
    """ Job statuses should return the correct status and error message to the user """
    sess = database.session
    sub = SubmissionFactory()
    sess.add(sub)

    # D1 generation waiting to be picked up by the Validator
    job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award_procurement'], submission=sub, error_message='',
                     filename='job_id/file.csv', original_filename='file.csv')
    val_job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                         file_type_id=FILE_TYPE_DICT['award_procurement'], submission=sub, error_message='',
                         number_of_errors=0)
    sess.add_all([job, val_job])
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # D1 generation running in the Validator
    job.job_status_id = JOB_STATUS_DICT['running']
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # D1 generation with an unknown error
    job.job_status_id = JOB_STATUS_DICT['failed']
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Upload job failed without error message'

    # D1 generation with a known error
    job.error_message = 'D1 upload error message'
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'D1 upload error message'

    # D1 generation completed by the Validator; validation waiting to be picked up
    job.error_message = ''
    job.job_status_id = JOB_STATUS_DICT['finished']
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # D1 generation completed; validation running in the Validator
    val_job.job_status_id = JOB_STATUS_DICT['running']
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # D1 generation completed; validation completed by the Validator
    val_job.job_status_id = JOB_STATUS_DICT['finished']
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'finished'

    # D1 generation completed; validation completed by the Validator
    val_job.number_of_errors = 10
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Validation completed but row-level errors were found'

    # D1 generation completed; validation with an unknown error
    job.error_message = ''
    val_job.error_message = ''
    val_job.job_status_id = JOB_STATUS_DICT['failed']
    val_job.number_of_errors = 0
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Validation job had an internal error'

    # D1 generation completed; validation with a known error
    job.error_message = ''
    val_job.error_message = ''
    val_job.error_message = 'D1 upload error message'
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'D1 upload error message'

    # D1 generation completed; validation with an unknown error
    job.error_message = ''
    val_job.error_message = ''
    val_job.job_status_id = JOB_STATUS_DICT['invalid']
    sess.commit()
    response_dict = file_generation_handler.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Generated file had file-level errors'
