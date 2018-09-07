import pytest

from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactbroker.helpers.generation_helper import check_file_generation
from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory


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
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # Detached D2 generation running in the Validator
    job.job_status_id = JOB_STATUS_DICT['running']
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # Detached D2 generation completed by the Validator
    job.job_status_id = JOB_STATUS_DICT['finished']
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'finished'
    assert response_dict['message'] == ''

    # Detached D2 generation with an unknown error
    job.job_status_id = JOB_STATUS_DICT['failed']
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Upload job failed without error message'

    # Detached D2 generation with a known error
    job.error_message = 'FABS upload error message'
    sess.commit()
    response_dict = check_file_generation(job.job_id)
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
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # D1 generation running in the Validator
    job.job_status_id = JOB_STATUS_DICT['running']
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # D1 generation with an unknown error
    job.job_status_id = JOB_STATUS_DICT['failed']
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Upload job failed without error message'

    # D1 generation with a known error
    job.error_message = 'D1 upload error message'
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'D1 upload error message'

    # D1 generation completed by the Validator; validation waiting to be picked up
    job.error_message = ''
    job.job_status_id = JOB_STATUS_DICT['finished']
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # D1 generation completed; validation running in the Validator
    val_job.job_status_id = JOB_STATUS_DICT['running']
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # D1 generation completed; validation completed by the Validator
    val_job.job_status_id = JOB_STATUS_DICT['finished']
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'finished'

    # D1 generation completed; validation completed by the Validator
    val_job.number_of_errors = 10
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Validation completed but row-level errors were found'

    # D1 generation completed; validation with an unknown error
    job.error_message = ''
    val_job.error_message = ''
    val_job.job_status_id = JOB_STATUS_DICT['failed']
    val_job.number_of_errors = 0
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Validation job had an internal error'

    # D1 generation completed; validation with a known error
    job.error_message = ''
    val_job.error_message = ''
    val_job.error_message = 'D1 upload error message'
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'D1 upload error message'

    # D1 generation completed; validation with an unknown error
    job.error_message = ''
    val_job.error_message = ''
    val_job.job_status_id = JOB_STATUS_DICT['invalid']
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Generated file had file-level errors'
