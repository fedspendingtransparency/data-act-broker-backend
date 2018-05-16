from datetime import datetime, timedelta
from unittest.mock import Mock

from dataactcore.models.jobModels import JobStatus, JobType, FileType
from dataactvalidator.validation_handlers import file_generation_handler
from dataactvalidator.validation_handlers.file_generation_manager import FileGenerationManager

from tests.unit.dataactcore.factories.job import JobFactory, FileRequestFactory


def test_generate_new_d1_file_success(monkeypatch, mock_broker_config_paths, database, job_constants):
    """ Testing that a new D1 file is generated """
    sess = database.session
    job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award_procurement').one(),
        filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
        start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True,
    )
    sess.add(job)
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123')

    sess.refresh(job)
    assert job.original_filename != 'original'
    assert job.from_cached is False
    assert job.job_status_id == sess.query(JobStatus).filter_by(name='finished').one().job_status_id


def test_generate_new_d2_file_success(monkeypatch, mock_broker_config_paths, database, job_constants):
    """ Testing that a new D2 file is generated """
    sess = database.session
    job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award').one(),
        filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
        start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True,
    )
    sess.add(job)
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123')

    sess.refresh(job)
    assert job.original_filename != 'original'
    assert job.from_cached is False
    assert job.job_status_id == sess.query(JobStatus).filter_by(name='finished').one().job_status_id


def test_regenerate_same_d1_file_success(monkeypatch, mock_broker_config_paths, database, job_constants):
    """Testing that a new file is not generated if this job already has a successfully generated file"""
    sess = database.session
    job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award_procurement').one(),
        filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
        start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True,
    )
    sess.add(job)
    sess.commit()

    file_request = FileRequestFactory(
        job=job, is_cached_file=True, agency_code='123', start_date='01/01/2017', end_date='01/31/2017',
        file_type='d1', request_date=datetime.now().date(),)
    sess.add(file_request)
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123')

    sess.refresh(job)
    assert job.original_filename == 'original'
    assert job.from_cached is False
    assert job.job_status_id == sess.query(JobStatus).filter_by(name='finished').one().job_status_id


def test_regenerate_same_d2_file_success(monkeypatch, mock_broker_config_paths, database, job_constants):
    """Testing that a new file is not generated if this job already has a successfully generated file"""
    sess = database.session
    job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award').one(),
        filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
        start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True,
    )
    sess.add(job)
    sess.commit()

    file_request = FileRequestFactory(
        job=job, is_cached_file=True, agency_code='123', start_date='01/01/2017', end_date='01/31/2017',
        file_type='d2', request_date=datetime.now().date(),)
    sess.add(file_request)
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123')

    sess.refresh(job)
    assert job.original_filename == 'original'
    assert job.from_cached is False
    assert job.job_status_id == sess.query(JobStatus).filter_by(name='finished').one().job_status_id


def test_regenerate_new_d1_file_success(monkeypatch, mock_broker_config_paths, database, job_constants):
    """Testing that a new file is not generated if another job has already has a successfully generated file"""
    sess = database.session
    original_job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='finished').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award_procurement').one(),
        filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
        start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True,
    )
    sess.add(original_job)
    sess.commit()

    file_request = FileRequestFactory(
        job=original_job, is_cached_file=True, agency_code='123', start_date='01/01/2017', end_date='01/31/2017',
        file_type='D1', request_date=datetime.now().date(),)
    new_job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award_procurement').one(),
        start_date='01/01/2017', end_date='01/31/2017',
    )
    sess.add_all([file_request, new_job])
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, new_job)))
    FileGenerationManager().generate_from_job(new_job.job_id, '123')

    sess.refresh(new_job)
    assert new_job.original_filename == 'original'
    assert new_job.from_cached is True
    assert new_job.job_status_id == sess.query(JobStatus).filter_by(name='finished').one().job_status_id


def test_regenerate_new_d2_file_success(monkeypatch, mock_broker_config_paths, database, job_constants):
    """Testing that a new file is not generated if another job has already has a successfully generated file"""
    sess = database.session
    original_job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='finished').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award').one(),
        filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
        start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True,
    )
    sess.add(original_job)
    sess.commit()

    file_request = FileRequestFactory(
        job=original_job, is_cached_file=True, agency_code='123', start_date='01/01/2017', end_date='01/31/2017',
        file_type='D2', request_date=datetime.now().date(),)
    new_job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award').one(),
        start_date='01/01/2017', end_date='01/31/2017',
    )
    sess.add_all([file_request, new_job])
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, new_job)))
    FileGenerationManager().generate_from_job(new_job.job_id, '123')

    sess.refresh(new_job)
    assert new_job.original_filename == 'original'
    assert new_job.from_cached is True
    assert new_job.job_status_id == sess.query(JobStatus).filter_by(name='finished').one().job_status_id


def test_uncache_same_d1_file_fpds_success(monkeypatch, mock_broker_config_paths, database, job_constants):
    """Testing that a new file is not generated if this job already has a successfully generated file"""
    sess = database.session
    job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award_procurement').one(),
        filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
        start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True,
    )
    sess.add(job)
    sess.commit()

    file_request = FileRequestFactory(
        job=job, is_cached_file=True, agency_code='123', start_date='01/01/2017', end_date='01/31/2017',
        file_type='d1', request_date=(datetime.now().date() - timedelta(1)),)
    sess.add(file_request)
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123')

    sess.refresh(job)
    assert job.original_filename != 'original'
    assert job.from_cached is False
    assert job.job_status_id == sess.query(JobStatus).filter_by(name='finished').one().job_status_id


def test_uncache_new_d1_file_fpds_success(monkeypatch, mock_broker_config_paths, database, job_constants):
    """Testing that a new file is not generated if another job has already has a successfully generated file"""
    sess = database.session
    original_job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='finished').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award_procurement').one(),
        filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
        start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True,
    )
    sess.add(original_job)
    sess.commit()

    file_request = FileRequestFactory(
        job=original_job, is_cached_file=True, agency_code='123', start_date='01/01/2017', end_date='01/31/2017',
        file_type='D1', request_date=(datetime.now().date() - timedelta(1)),)
    new_job = JobFactory(
        job_status=sess.query(JobStatus).filter_by(name='waiting').one(),
        job_type=sess.query(JobType).filter_by(name='file_upload').one(),
        file_type=sess.query(FileType).filter_by(name='award_procurement').one(),
        start_date='01/01/2017', end_date='01/31/2017',
    )
    sess.add_all([file_request, new_job])
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, new_job)))
    FileGenerationManager().generate_from_job(new_job.job_id, '123')

    sess.refresh(new_job)
    assert new_job.original_filename != 'original'
    assert new_job.from_cached is False
    assert new_job.job_status_id == sess.query(JobStatus).filter_by(name='finished').one().job_status_id
