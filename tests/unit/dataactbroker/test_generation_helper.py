import pytest

from datetime import datetime, date
from unittest.mock import Mock

from dataactbroker.helpers import generation_helper
from dataactbroker.helpers.generation_helper import (
    check_file_generation, check_generation_prereqs, copy_file_generation_to_job, start_d_generation,
    retrieve_cached_file_generation)

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.models.jobModels import FileGeneration
from dataactcore.utils.responseException import ResponseException

from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory, FileGenerationFactory


@pytest.mark.usefixtures("job_constants")
def test_start_d_generation_submission_cached(database, monkeypatch):
    """ Cached D files must update the upload Job with the FileGeneration data. """
    sess = database.session
    original_filename = 'D1_test_gen.csv'
    file_path = gen_file_path_from_submission('None/', original_filename)

    submission = SubmissionFactory(
        submission_id=1000, reporting_start_date='2017-01-01', reporting_end_date='2017-01-31', frec_code='1234',
        cgac_code=None, is_quarter_format=False, publishable=False, reporting_fiscal_year='2017')
    file_gen = FileGenerationFactory(
        request_date=datetime.now().date(), start_date='2017-01-01', end_date='2017-01-31', file_type='D2',
        agency_code='1234', agency_type='awarding', is_cached_file=True, file_path=file_path)
    up_job = JobFactory(
        job_status_id=JOB_STATUS_DICT['waiting'], file_type_id=FILE_TYPE_DICT['award'], error_message=None,
        job_type_id=JOB_TYPE_DICT['file_upload'], filename=None, original_filename=None,
        submission_id=submission.submission_id)
    val_job = JobFactory(
        job_status_id=JOB_STATUS_DICT['waiting'], error_message=None, file_type_id=FILE_TYPE_DICT['award'],
        job_type_id=JOB_TYPE_DICT['csv_record_validation'], filename=None, original_filename=None,
        submission_id=submission.submission_id)
    sess.add_all([submission, file_gen, up_job, val_job])
    sess.commit()

    monkeypatch.setattr(generation_helper, 'g', Mock(return_value={'is_local': CONFIG_BROKER['local']}))
    start_d_generation(up_job, '01/01/2017', '01/31/2017', 'awarding')

    assert up_job.file_generation_id == file_gen.file_generation_id
    assert up_job.start_date == date(2017, 1, 1)
    assert up_job.end_date == date(2017, 1, 31)
    assert up_job.original_filename == original_filename
    assert up_job.filename == gen_file_path_from_submission(up_job.submission_id, original_filename)
    assert up_job.job_status_id == JOB_STATUS_DICT['finished']

    assert up_job.start_date == date(2017, 1, 1)
    assert up_job.end_date == date(2017, 1, 31)
    assert up_job.original_filename == original_filename
    assert up_job.filename == gen_file_path_from_submission(up_job.submission_id, original_filename)
    assert up_job.job_status_id != JOB_STATUS_DICT['waiting']


@pytest.mark.usefixtures("job_constants")
def test_start_d_generation_submission_change_request(database, monkeypatch):
    """ In-submission generations that change their requested start or end dates must actually generate files based on
        the new dates.
    """
    sess = database.session
    original_filename = 'D1_test_gen.csv'
    file_path = gen_file_path_from_submission('None/', original_filename)

    submission = SubmissionFactory(
        submission_id=1000, reporting_start_date='2017-01-01', reporting_end_date='2017-01-31', cgac_code='123',
        frec_code=None, is_quarter_format=False, publishable=False, reporting_fiscal_year='2017')
    file_gen = FileGenerationFactory(
        request_date=datetime.now().date(), start_date='2017-01-01', end_date='2017-01-31', file_type='D1',
        agency_code='123', agency_type='awarding', is_cached_file=True, file_path=file_path, file_generation_id=1000)
    up_job = JobFactory(
        job_status_id=JOB_STATUS_DICT['waiting'], error_message=None, job_type_id=JOB_TYPE_DICT['file_upload'],
        file_type_id=FILE_TYPE_DICT['award_procurement'], filename=None, submission_id=submission.submission_id,
        file_generation_id=file_gen.file_generation_id, original_filename=original_filename)
    val_job = JobFactory(
        job_status_id=JOB_STATUS_DICT['waiting'], error_message=None, file_type_id=FILE_TYPE_DICT['award_procurement'],
        job_type_id=JOB_TYPE_DICT['csv_record_validation'], filename=None, submission_id=submission.submission_id,
        original_filename=original_filename)
    sess.add_all([submission, file_gen, up_job, val_job])
    sess.commit()

    monkeypatch.setattr(generation_helper, 'g', Mock(return_value={'is_local': CONFIG_BROKER['local']}))
    start_d_generation(up_job, '01/01/2017', '01/30/2017', 'funding')

    assert up_job.file_generation_id != file_gen.file_generation_id
    assert up_job.start_date == date(2017, 1, 1)
    assert up_job.end_date == date(2017, 1, 30)
    assert up_job.original_filename != original_filename
    assert up_job.filename != gen_file_path_from_submission(up_job.submission_id, original_filename)

    assert up_job.start_date == date(2017, 1, 1)
    assert up_job.end_date == date(2017, 1, 30)
    assert up_job.original_filename == up_job.original_filename
    assert up_job.filename == up_job.filename


@pytest.mark.usefixtures("job_constants")
def test_start_d_generation_submission_new(database, monkeypatch):
    """ A new file generation must update the upload Job and create a new FileGeneration object. """
    sess = database.session
    original_filename = 'D2_test_gen.csv'

    submission = SubmissionFactory(
        submission_id=1000, reporting_start_date='2017-01-01', reporting_end_date='2017-01-31', cgac_code='123',
        frec_code=None, is_quarter_format=False, publishable=False, reporting_fiscal_year='2017')
    up_job = JobFactory(
        job_status_id=JOB_STATUS_DICT['waiting'], error_message=None, file_type_id=FILE_TYPE_DICT['award'],
        job_type_id=JOB_TYPE_DICT['file_upload'], filename=None, submission_id=submission.submission_id,
        original_filename=original_filename, file_generation_id=None)
    val_job = JobFactory(
        job_status_id=JOB_STATUS_DICT['waiting'], error_message=None, file_type_id=FILE_TYPE_DICT['award'],
        job_type_id=JOB_TYPE_DICT['csv_record_validation'],  filename=None, submission_id=submission.submission_id,
        original_filename=original_filename)
    sess.add_all([submission, up_job, val_job])
    sess.commit()

    monkeypatch.setattr(generation_helper, 'g', Mock(return_value={'is_local': CONFIG_BROKER['local']}))
    start_d_generation(up_job, '01/01/2017', '01/31/2017', 'awarding')

    assert up_job.file_generation_id is not None
    assert up_job.start_date == date(2017, 1, 1)
    assert up_job.end_date == date(2017, 1, 31)
    assert up_job.original_filename != original_filename
    assert up_job.filename != gen_file_path_from_submission(up_job.submission_id, original_filename)

    assert up_job.start_date == date(2017, 1, 1)
    assert up_job.end_date == date(2017, 1, 31)
    assert up_job.original_filename == up_job.original_filename
    assert up_job.filename == up_job.filename

    file_gen = sess.query(FileGeneration).filter_by(file_generation_id=up_job.file_generation_id).one_or_none()
    assert file_gen is not None
    assert file_gen.request_date == datetime.now().date()
    assert file_gen.start_date == date(2017, 1, 1)
    assert file_gen.end_date == date(2017, 1, 31)
    assert file_gen.file_type == 'D2'
    assert file_gen.file_path != gen_file_path_from_submission('None', original_filename)


@pytest.mark.usefixtures("job_constants")
def test_retrieve_cached_file_generation(database):
    """ Should successfully return the correct cached FileGeneration """
    sess = database.session
    job = JobFactory(
        start_date='2017-01-01', end_date='2017-01-31', job_status_id=JOB_STATUS_DICT['waiting'], error_message=None,
        file_type_id=FILE_TYPE_DICT['award'], job_type_id=JOB_TYPE_DICT['file_upload'], filename=None,
        original_filename=None, file_generation_id=None)
    file_gen = FileGenerationFactory(
        request_date=datetime.now().date(), start_date='2017-01-01', end_date='2017-01-31', file_type='D2',
        agency_code='123', agency_type='awarding', is_cached_file=True)
    sess.add_all([job, file_gen])
    sess.commit()

    file_generation = retrieve_cached_file_generation(job, 'awarding', '123')
    assert file_generation == file_gen


@pytest.mark.usefixtures("job_constants")
def test_retrieve_cached_file_generation_none(database):
    """ Should return no FileGeneration """
    sess = database.session
    job = JobFactory(
        start_date='2017-01-01', end_date='2017-01-31', job_status_id=JOB_STATUS_DICT['waiting'], error_message=None,
        file_type_id=FILE_TYPE_DICT['award'], job_type_id=JOB_TYPE_DICT['file_upload'], filename=None,
        original_filename=None, file_generation_id=None)
    sess.add(job)
    sess.commit()

    file_generation = retrieve_cached_file_generation(job, 'awarding', '123')
    assert file_generation is None


@pytest.mark.usefixtures("job_constants")
def test_retrieve_cached_file_generation_end_date_diff(database):
    """ Should return no FileGeneration """
    sess = database.session
    job = JobFactory(
        start_date='2017-01-01', end_date='2017-01-31', job_status_id=JOB_STATUS_DICT['waiting'], error_message=None,
        file_type_id=FILE_TYPE_DICT['award'], job_type_id=JOB_TYPE_DICT['file_upload'], filename=None,
        original_filename=None, file_generation_id=None)
    file_gen = FileGenerationFactory(
        request_date=datetime.now().date(), start_date='2017-01-01', end_date='2017-01-30', file_type='D2',
        agency_code='123', agency_type='awarding', is_cached_file=True)
    sess.add_all([job, file_gen])
    sess.commit()

    file_generation = retrieve_cached_file_generation(job, 'awarding', '123')
    assert file_generation is None


@pytest.mark.usefixtures("job_constants")
def test_retrieve_cached_file_generation_start_date_diff(database):
    """ Should return no FileGeneration """
    sess = database.session
    job = JobFactory(
        start_date='2017-01-01', end_date='2017-01-31', job_status_id=JOB_STATUS_DICT['waiting'], error_message=None,
        file_type_id=FILE_TYPE_DICT['award'], job_type_id=JOB_TYPE_DICT['file_upload'], filename=None,
        original_filename=None, file_generation_id=None)
    file_gen = FileGenerationFactory(
        request_date=datetime.now().date(), start_date='2017-01-02', end_date='2017-01-31', file_type='D2',
        agency_code='123', agency_type='awarding', is_cached_file=True)
    sess.add_all([job, file_gen])
    sess.commit()

    file_generation = retrieve_cached_file_generation(job, 'awarding', '123')
    assert file_generation is None


@pytest.mark.usefixtures("job_constants")
def test_retrieve_cached_file_generation_agency_code_diff(database):
    """ Should return no FileGeneration """
    sess = database.session
    job = JobFactory(
        start_date='2017-01-01', end_date='2017-01-31', job_status_id=JOB_STATUS_DICT['waiting'], error_message=None,
        file_type_id=FILE_TYPE_DICT['award'], job_type_id=JOB_TYPE_DICT['file_upload'], filename=None,
        original_filename=None, file_generation_id=None)
    file_gen = FileGenerationFactory(
        request_date=datetime.now().date(), start_date='2017-01-01', end_date='2017-01-31', file_type='D2',
        agency_code='124', agency_type='awarding', is_cached_file=True)
    sess.add_all([job, file_gen])
    sess.commit()

    file_generation = retrieve_cached_file_generation(job, 'awarding', '123')
    assert file_generation is None


@pytest.mark.usefixtures("job_constants")
def test_retrieve_cached_file_generation_agency_type_diff(database):
    """ Should return no FileGeneration """
    sess = database.session
    job = JobFactory(
        start_date='2017-01-01', end_date='2017-01-31', job_status_id=JOB_STATUS_DICT['waiting'], error_message=None,
        file_type_id=FILE_TYPE_DICT['award'], job_type_id=JOB_TYPE_DICT['file_upload'], filename=None,
        original_filename=None, file_generation_id=None)
    file_gen = FileGenerationFactory(
        request_date=datetime.now().date(), start_date='2017-01-01', end_date='2017-01-31', file_type='D2',
        agency_code='123', agency_type='awarding', is_cached_file=True)
    sess.add_all([job, file_gen])
    sess.commit()

    file_generation = retrieve_cached_file_generation(job, 'funding', '123')
    assert file_generation is None


@pytest.mark.usefixtures("job_constants")
def test_retrieve_cached_file_generation_file_type_diff(database):
    """ Should return no FileGeneration """
    sess = database.session
    job = JobFactory(
        start_date='2017-01-01', end_date='2017-01-31', job_status_id=JOB_STATUS_DICT['waiting'], error_message=None,
        file_type_id=FILE_TYPE_DICT['award'], job_type_id=JOB_TYPE_DICT['file_upload'], filename=None,
        original_filename=None, file_generation_id=None)
    file_gen = FileGenerationFactory(
        request_date=datetime.now().date(), start_date='2017-01-01', end_date='2017-01-31', file_type='D1',
        agency_code='123', agency_type='awarding', is_cached_file=True)
    sess.add_all([job, file_gen])
    sess.commit()

    file_generation = retrieve_cached_file_generation(job, 'awarding', '123')
    assert file_generation is None


@pytest.mark.usefixtures("job_constants")
def test_retrieve_cached_file_generation_not_cached(database):
    """ Should return no FileGeneration """
    sess = database.session
    job = JobFactory(
        start_date='2017-01-01', end_date='2017-01-31', job_status_id=JOB_STATUS_DICT['waiting'], error_message=None,
        file_type_id=FILE_TYPE_DICT['award'], job_type_id=JOB_TYPE_DICT['file_upload'], filename=None,
        original_filename=None, file_generation_id=None)
    file_gen = FileGenerationFactory(
        request_date=datetime.now().date(), start_date='2017-01-01', end_date='2017-01-31', file_type='D2',
        agency_code='123', agency_type='awarding', is_cached_file=False)
    sess.add_all([job, file_gen])
    sess.commit()

    file_generation = retrieve_cached_file_generation(job, 'awarding', '123')
    assert file_generation is None


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
def test_copy_file_generation_to_job(monkeypatch, database):
    sess = database.session
    original_filename = 'new_filename.csv'
    file_path = gen_file_path_from_submission('None', original_filename)

    job = JobFactory(job_status_id=JOB_STATUS_DICT['running'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award'])
    file_gen = FileGenerationFactory(file_type='D1', file_path=file_path)
    sess.add_all([job, file_gen])
    sess.commit()

    monkeypatch.setattr(generation_helper, 'g', Mock(return_value={'is_local': CONFIG_BROKER['local']}))
    copy_file_generation_to_job(job, file_gen, True)
    sess.refresh(job)
    sess.refresh(file_gen)

    assert job.job_status.name == 'finished'
    assert job.filename == gen_file_path_from_submission(job.submission_id, original_filename)
    assert job.original_filename == original_filename
    assert job.number_of_errors == 0
    assert job.number_of_warnings == 0
    assert job.file_generation_id == file_gen.file_generation_id


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


def gen_file_path_from_submission(submission, original_filename):
    local_filepath = CONFIG_BROKER['broker_files']
    nonlocal_filepath = '{}/'.format(submission)
    return '{}{}'.format(local_filepath if CONFIG_BROKER['local'] else nonlocal_filepath, original_filename)


@pytest.mark.usefixtures("job_constants")
def test_check_detached_a_file_generation(database):
    """ Job statuses should return the correct status and error message to the user """
    sess = database.session

    # Detached A generation waiting to be picked up by the Validator
    job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['appropriations'], error_message='', filename='job_id/file.csv',
                     original_filename='file.csv')
    sess.add(job)
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # Detached A generation running in the Validator
    job.job_status_id = JOB_STATUS_DICT['running']
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'waiting'

    # Detached A generation completed by the Validator
    job.job_status_id = JOB_STATUS_DICT['finished']
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'finished'
    assert response_dict['message'] == ''

    # Detached A generation with an unknown error
    job.job_status_id = JOB_STATUS_DICT['failed']
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'Upload job failed without error message'

    # Detached A generation with a known error
    job.error_message = 'A file upload error message'
    sess.commit()
    response_dict = check_file_generation(job.job_id)
    assert response_dict['status'] == 'failed'
    assert response_dict['message'] == 'A file upload error message'
