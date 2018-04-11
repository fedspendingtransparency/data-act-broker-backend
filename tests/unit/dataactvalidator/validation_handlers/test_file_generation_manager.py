import csv
import os
import re

from collections import OrderedDict
from unittest.mock import Mock

from dataactcore.models.jobModels import FileType, JobStatus, JobType
from dataactvalidator.validation_handlers import file_generation_manager
from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory


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

    with file_generation_manager.job_context(job.job_id, is_local=True):
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
        error_message=None,
    )
    sess.add(job)
    sess.commit()

    with file_generation_manager.job_context(job.job_id, is_local=True):
        raise Exception('This failed!')

    sess.refresh(job)
    assert job.job_status.name == 'failed'
    assert job.error_message == 'This failed!'

def test_check_detached_d_file_generation(database, job_constants):
    """Job statuses should return the correct status and error message to the user"""
    sess = database.session

    # Detached D2 generation waiting to be picked up by the Validator
    job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award').one(),
        error_message='',
    )
    sess.add(job)
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # Detached D2 generation running in the Validator
    job.job_status = sess.query(JobStatus).filter_by(name='running').one()
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # Detached D2 generation completed by the Validator
    job.job_status = sess.query(JobStatus).filter_by(name='finished').one()
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'finished'
    assert response_dict['message'] == ''

    # Detached D2 generation with an unknown error
    job.job_status = sess.query(JobStatus).filter_by(name='failed').one()
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Upload job failed without error message'

    # Detached D2 generation with a known error
    job.error_message = 'Detached D2 upload error message'
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Detached D2 upload error message'


def test_check_submission_d_file_generation(database, job_constants):
    """Job statuses should return the correct status and error message to the user"""
    sess = database.session
    sub = SubmissionFactory()
    sess.add(sub)

    # D1 generation waiting to be picked up by the Validator
    job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award_procurement').one(),
        submission=sub, error_message=''
    )
    val_job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
        job_type=sess.query(JobType).filter_by(name='csv_record_validation').one(),
        file_type=sess.query(FileType).filter_by(name='award_procurement').one(),
        submission=sub, error_message='', number_of_errors=0,
    )
    sess.add_all([job, val_job])
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # D1 generation running in the Validator
    job.job_status = sess.query(JobStatus).filter_by(name='running').one()
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # D1 generation with an unknown error
    job.job_status = sess.query(JobStatus).filter_by(name='failed').one()
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Upload job failed without error message'

    # D1 generation with a known error
    job.error_message = 'D1 upload error message'
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'D1 upload error message'

    # D1 generation completed by the Validator; validation waiting to be picked up
    job.error_message = ''
    job.job_status = sess.query(JobStatus).filter_by(name='finished').one()
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # D1 generation completed; validation running in the Validator
    val_job.job_status = sess.query(JobStatus).filter_by(name='running').one()
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # D1 generation completed; validation completed by the Validator
    val_job.job_status = sess.query(JobStatus).filter_by(name='finished').one()
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'finished'

    # D1 generation completed; validation completed by the Validator
    val_job.number_of_errors = 10
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Validation completed but row-level errors were found'

    # D1 generation completed; validation with an unknown error
    job.error_message = ''
    val_job.error_message = ''
    val_job.job_status = sess.query(JobStatus).filter_by(name='failed').one()
    val_job.number_of_errors = 0
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Validation job had an internal error'

    # D1 generation completed; validation with a known error
    job.error_message = ''
    val_job.error_message = ''
    val_job.error_message = 'D1 upload error message'
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'D1 upload error message'

    # D1 generation completed; validation with an unknown error
    job.error_message = ''
    val_job.error_message = ''
    val_job.job_status = sess.query(JobStatus).filter_by(name='invalid').one()
    sess.commit()
    response_dict = file_generation_manager.check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Generated file had file-level errors'
