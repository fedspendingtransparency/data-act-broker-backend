import pytest

from dataactbroker.helpers.generation_helper import (check_file_generation, check_generation_prereqs,
                                                     copy_parent_file_request_data)

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.utils.responseException import ResponseException

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


@pytest.mark.usefixtures("job_constants")
def test_copy_parent_file_request_data(database):
    sess = database.session

    job_one = JobFactory(job_status_id=JOB_STATUS_DICT['finished'], job_type_id=JOB_TYPE_DICT['file_upload'],
                         file_type_id=FILE_TYPE_DICT['award'])
    job_two = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                         file_type_id=FILE_TYPE_DICT['award'], filename='None/new_filename.csv')
    sess.add_all([job_one, job_two])
    sess.commit()

    copy_parent_file_request_data(job_two, job_one, True)
    sess.refresh(job_one)
    sess.refresh(job_two)

    assert job_two.job_status_id == job_one.job_status_id
    filepath = CONFIG_BROKER['broker_files'] if CONFIG_BROKER['local'] else "{}/".format(str(job_two.submission_id))
    assert job_two.filename == '{}{}'.format(filepath, job_one.original_filename)
    assert job_two.original_filename == job_one.original_filename
    assert job_two.number_of_errors == job_one.number_of_errors
    assert job_two.number_of_warnings == job_one.number_of_warnings
    assert job_two.from_cached is True


@pytest.mark.usefixtures("job_constants")
def test_check_generation_prereqs_ef_valid(database):
    """ Tests a set of conditions that passes the prerequisite checks to allow E/F files to be generated. Show that
        warnings do not prevent generation.
    """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    cross_val = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['validation'],
                           file_type_id=None, job_status_id=JOB_STATUS_DICT['finished'], number_of_errors=0,
                           number_of_warnings=1, error_message=None)
    sess.add_all([sub, cross_val])
    sess.commit()

    can_generate = check_generation_prereqs(sub.submission_id, 'E')
    assert can_generate is True


@pytest.mark.usefixtures("job_constants")
def test_check_generation_prereqs_ef_not_finished(database):
    """ Tests a set of conditions that has cross-file still waiting, fail the generation check for E/F files. """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    cross_val = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['validation'], file_type_id=None,
                           job_status_id=JOB_STATUS_DICT['waiting'], number_of_errors=0, number_of_warnings=0,
                           error_message=None)
    sess.add_all([sub, cross_val])
    sess.commit()

    can_generate = check_generation_prereqs(sub.submission_id, 'E')
    assert can_generate is False


@pytest.mark.usefixtures("job_constants")
def test_check_generation_prereqs_ef_has_errors(database):
    """ Tests a set of conditions that has an error in cross-file, fail the generation check for E/F files. """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    cross_val = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['validation'], file_type_id=None,
                           job_status_id=JOB_STATUS_DICT['finished'], number_of_errors=1, number_of_warnings=0,
                           error_message=None)
    sess.add_all([sub, cross_val])
    sess.commit()

    can_generate = check_generation_prereqs(sub.submission_id, 'E')
    assert can_generate is False


@pytest.mark.usefixtures("job_constants")
def test_check_generation_prereqs_d_valid(database):
    """ Tests a set of conditions that passes the prerequisite checks to allow D files to be generated. Show that
        warnings do not prevent generation.
    """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    job_1 = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                       file_type_id=FILE_TYPE_DICT['appropriations'], job_status_id=JOB_STATUS_DICT['finished'],
                       number_of_errors=0, number_of_warnings=0, error_message=None)
    job_2 = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                       file_type_id=FILE_TYPE_DICT['program_activity'], job_status_id=JOB_STATUS_DICT['finished'],
                       number_of_errors=0, number_of_warnings=0, error_message=None)
    job_3 = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                       file_type_id=FILE_TYPE_DICT['award_financial'], job_status_id=JOB_STATUS_DICT['finished'],
                       number_of_errors=0, number_of_warnings=1, error_message=None)
    sess.add_all([sub, job_1, job_2, job_3])
    sess.commit()

    can_generate = check_generation_prereqs(sub.submission_id, 'D1')
    assert can_generate is True


@pytest.mark.usefixtures("job_constants")
def test_check_generation_prereqs_d_not_finished(database):
    """ Tests a set of conditions that has one of the A,B,C files incomplete, prevent D file generation. """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    job_1 = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                       file_type_id=FILE_TYPE_DICT['appropriations'], job_status_id=JOB_STATUS_DICT['finished'],
                       number_of_errors=0, number_of_warnings=0, error_message=None)
    job_2 = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                       file_type_id=FILE_TYPE_DICT['program_activity'], job_status_id=JOB_STATUS_DICT['waiting'],
                       number_of_errors=0, number_of_warnings=0, error_message=None)
    job_3 = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                       file_type_id=FILE_TYPE_DICT['award_financial'], job_status_id=JOB_STATUS_DICT['finished'],
                       number_of_errors=0, number_of_warnings=0, error_message=None)
    sess.add_all([sub, job_1, job_2, job_3])
    sess.commit()

    can_generate = check_generation_prereqs(sub.submission_id, 'D1')
    assert can_generate is False


@pytest.mark.usefixtures("job_constants")
def test_check_generation_prereqs_d_has_errors(database):
    """ Tests a set of conditions that has an error in one of the A,B,C files, prevent D file generation. """
    sess = database.session

    sub = SubmissionFactory(submission_id=1, d2_submission=False)
    job_1 = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                       file_type_id=FILE_TYPE_DICT['appropriations'], job_status_id=JOB_STATUS_DICT['finished'],
                       number_of_errors=1, number_of_warnings=0, error_message=None)
    job_2 = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                       file_type_id=FILE_TYPE_DICT['program_activity'], job_status_id=JOB_STATUS_DICT['finished'],
                       number_of_errors=0, number_of_warnings=0, error_message=None)
    job_3 = JobFactory(submission_id=sub.submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                       file_type_id=FILE_TYPE_DICT['award_financial'], job_status_id=JOB_STATUS_DICT['finished'],
                       number_of_errors=0, number_of_warnings=0, error_message=None)
    sess.add_all([sub, job_1, job_2, job_3])
    sess.commit()

    can_generate = check_generation_prereqs(sub.submission_id, 'D1')
    assert can_generate is False


@pytest.mark.usefixtures("job_constants")
def test_check_generation_prereqs_bad_type(database):
    """ Tests that check_generation_prereqs raises an error if an invalid type is provided. """
    sess = database.session
    sub = SubmissionFactory()
    sess.add(sub)
    sess.commit()

    with pytest.raises(ResponseException):
        check_generation_prereqs(sub.submission_id, 'A')
