from collections import OrderedDict
import csv
import os
from unittest.mock import Mock

from celery.exceptions import MaxRetriesExceededError, Retry
import pytest

from dataactcore.interfaces.interfaceHolder import InterfaceHolder
from dataactcore.models.jobModels import FileType, JobStatus, JobType
from dataactcore.utils import fileE, jobQueue
from tests.unit.dataactcore.factories.staging import (
    AwardFinancialAssistanceFactory, AwardProcurementFactory)
from tests.unit.dataactcore.factories.job import JobFactory


def read_file_rows(file_path):
    assert os.path.isfile(file_path)

    with open(file_path) as f:
        return [row for row in csv.reader(f)]


def test_generate_f_file(monkeypatch, mock_broker_config_paths):
    """A CSV with fields in the right order should be written to the file
    system"""
    fileF_mock = Mock()
    monkeypatch.setattr(jobQueue, 'fileF', fileF_mock)
    fileF_mock.generateFRows.return_value = [
        dict(key4='a', key11='b'), dict(key4='c', key11='d')
    ]

    fileF_mock.mappings = OrderedDict(
        [('key4', 'mapping4'), ('key11', 'mapping11')])
    file_path = str(mock_broker_config_paths['broker_files'].join('uniq1'))
    expected = [['key4', 'key11'], ['a', 'b'], ['c', 'd']]
    jobQueue.generate_f_file(1, 1, Mock(), 'uniq1', 'uniq1', is_local=True)
    assert read_file_rows(file_path) == expected

    # re-order
    fileF_mock.mappings = OrderedDict(
        [('key11', 'mapping11'), ('key4', 'mapping4')])
    file_path = str(mock_broker_config_paths['broker_files'].join('uniq2'))
    expected = [['key11', 'key4'], ['b', 'a'], ['d', 'c']]
    jobQueue.generate_f_file(1, 1, Mock(), 'uniq2', 'uniq2', is_local=True)
    assert read_file_rows(file_path) == expected


def test_generate_e_file_query(monkeypatch, mock_broker_config_paths,
                               database):
    """Verify that generate_e_file makes an appropriate query (matching both
    D1 and D2 entries)"""
    # Generate several file D1 entries, largely with the same submission_id,
    # and with two overlapping DUNS. Generate several D2 entries with the same
    # submission_id as well
    model = AwardProcurementFactory()
    aps = [AwardProcurementFactory(submission_id=model.submission_id)
           for i in range(4)]
    afas = [AwardFinancialAssistanceFactory(submission_id=model.submission_id)
            for i in range(5)]
    same_duns = AwardProcurementFactory(
        submission_id=model.submission_id,
        awardee_or_recipient_uniqu=model.awardee_or_recipient_uniqu)
    unrelated = AwardProcurementFactory(submission_id=model.submission_id + 1)
    database.session.add_all(aps + afas + [model, same_duns, unrelated])

    monkeypatch.setattr(jobQueue.fileE, 'retrieveRows', Mock(return_value=[]))

    # Mock out the interface holder class; rather nasty, as we want to _keep_
    # the database session handler
    interface_class = Mock()
    interface_class.return_value.jobDb.session = database.session
    jobQueue.generate_e_file(
        model.submission_id, 1, interface_class, 'uniq', 'uniq',
        is_local=True)

    # [0][0] gives us the first, non-keyword args
    call_args = jobQueue.fileE.retrieveRows.call_args[0][0]
    expected = [ap.awardee_or_recipient_uniqu for ap in aps]
    expected.append(model.awardee_or_recipient_uniqu)
    expected.extend(afa.awardee_or_recipient_uniqu for afa in afas)
    assert list(sorted(call_args)) == list(sorted(expected))


def test_generate_e_file_csv(monkeypatch, mock_broker_config_paths, database):
    """Verify that an appropriate CSV is written, based on fileE.Row's
    structure"""
    monkeypatch.setattr(jobQueue.fileE, 'retrieveRows', Mock())
    jobQueue.fileE.retrieveRows.return_value = [
        fileE.Row('a', 'b', 'c', '1a', '1b', '2a', '2b', '3a', '3b',
                  '4a', '4b', '5a', '5b'),
        fileE.Row('A', 'B', 'C', '1A', '1B', '2A', '2B', '3A', '3B',
                  '4A', '4B', '5A', '5B')
    ]

    interface_class = Mock()
    interface_class.return_value.jobDb.session = database.session
    jobQueue.generate_e_file(
        1, 1, interface_class, 'uniq', 'uniq', is_local=True)

    file_path = str(mock_broker_config_paths['broker_files'].join('uniq'))
    expected = [
        ['AwardeeOrRecipientUniqueIdentifier',
         'UltimateParentUniqueIdentifier',
         'UltimateParentLegalEntityName',
         'HighCompOfficer1Name', 'HighCompOfficer1Amount',
         'HighCompOfficer2Name', 'HighCompOfficer2Amount',
         'HighCompOfficer3Name', 'HighCompOfficer3Amount',
         'HighCompOfficer4Name', 'HighCompOfficer4Amount',
         'HighCompOfficer5Name', 'HighCompOfficer5Amount'],
        ['a', 'b', 'c', '1a', '1b', '2a', '2b', '3a', '3b', '4a', '4b',
         '5a', '5b'],
        ['A', 'B', 'C', '1A', '1B', '2A', '2B', '3A', '3B', '4A', '4B',
         '5A', '5B']
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

    with jobQueue.job_context(Mock(), InterfaceHolder, job.job_id):
        pass    # i.e. be successful

    sess.refresh(job)
    assert job.job_status.name == 'finished'


def test_job_context_fail(database, job_constants):
    """When a job raises an exception and has no retries left, it should be
    marked as failed"""
    sess = database.session
    job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='running').one(),
        job_type=sess.query(JobType).filter_by(name='validation').one(),
        file_type=sess.query(FileType).filter_by(name='sub_award').one(),
    )
    sess.add(job)
    sess.commit()

    task = Mock()
    task.retry.return_value = MaxRetriesExceededError()
    with jobQueue.job_context(task, InterfaceHolder, job.job_id):
        raise Exception('This failed!')

    sess.refresh(job)
    assert job.job_status.name == 'failed'
    assert job.error_message == 'This failed!'


def test_job_context_retry(database, job_constants):
    """When a job raises an exception but can still retry, we should expect a
    particular exception (which signifies to celery that it should retry)"""
    sess = database.session
    job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='running').one(),
        job_type=sess.query(JobType).filter_by(name='validation').one(),
        file_type=sess.query(FileType).filter_by(name='sub_award').one(),
    )
    sess.add(job)
    sess.commit()

    task = Mock()
    task.retry.return_value = Retry()
    with pytest.raises(Retry):
        with jobQueue.job_context(task, InterfaceHolder, job.job_id):
            raise Exception('This failed!')

    sess.refresh(job)
    assert job.job_status.name == 'running'     # still going
