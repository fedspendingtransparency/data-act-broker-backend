import pytest

from datetime import datetime, timedelta
from unittest.mock import Mock

from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.models.jobModels import FileRequest
from dataactvalidator.validation_handlers import file_generation_handler
from dataactvalidator.validation_handlers.file_generation_manager import FileGenerationManager

from tests.unit.dataactcore.factories.job import JobFactory, FileRequestFactory


@pytest.mark.usefixtures("job_constants")
def test_generate_new_d1_file_success(monkeypatch, mock_broker_config_paths, database):
    """ Testing that a new D1 file is generated """
    sess = database.session
    job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award_procurement'],
                     filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
                     start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True)
    sess.add(job)
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123', 'awarding')

    sess.refresh(job)
    file_request = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id).one_or_none()
    assert file_request is not None
    assert file_request.is_cached_file is True
    assert file_request.start_date == job.start_date
    assert file_request.end_date == job.end_date
    assert file_request.agency_code == '123'
    assert file_request.request_date == datetime.now().date()

    assert job.original_filename != 'original'
    assert job.from_cached is False
    assert job.job_status_id == JOB_STATUS_DICT['finished']


@pytest.mark.usefixtures("job_constants")
def test_generate_new_d1_file_funding_success(monkeypatch, mock_broker_config_paths, database):
    """ Testing that a new D1 file is generated using funding agency """
    sess = database.session
    job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award_procurement'],
                     filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
                     start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True)
    sess.add(job)
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123', 'funding')

    sess.refresh(job)
    file_request = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id).one_or_none()
    assert file_request is not None
    assert file_request.is_cached_file is True
    assert file_request.start_date == job.start_date
    assert file_request.end_date == job.end_date
    assert file_request.agency_code == '123'
    assert file_request.request_date == datetime.now().date()
    assert file_request.agency_type == 'funding'

    assert job.original_filename != 'original'
    assert job.from_cached is False
    assert job.job_status_id == JOB_STATUS_DICT['finished']


@pytest.mark.usefixtures("job_constants")
def test_generate_new_d2_file_success(monkeypatch, mock_broker_config_paths, database):
    """ Testing that a new D2 file is generated """
    sess = database.session
    job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award'],
                     filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
                     start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True)
    sess.add(job)
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123', 'awarding')

    sess.refresh(job)
    file_request = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id).one_or_none()
    assert file_request is not None
    assert file_request.is_cached_file is True
    assert file_request.start_date == job.start_date
    assert file_request.end_date == job.end_date
    assert file_request.agency_code == '123'
    assert file_request.request_date == datetime.now().date()

    assert job.original_filename != 'original'
    assert job.from_cached is False
    assert job.job_status_id == JOB_STATUS_DICT['finished']


@pytest.mark.usefixtures("job_constants")
def test_generate_noncached_d1_file_success(monkeypatch, mock_broker_config_paths, database):
    """ Testing that a new D1 file is generated """
    sess = database.session
    job1 = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['award_procurement'],
                      filename=str(mock_broker_config_paths['d_file_storage_path'].join('diff_agency')),
                      start_date='01/01/2017', end_date='01/31/2017', original_filename='diff_agency',
                      from_cached=False)
    job2 = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['award'],
                      filename=str(mock_broker_config_paths['d_file_storage_path'].join('diff_start_date')),
                      start_date='01/02/2017', end_date='01/31/2017', original_filename='diff_start_date',
                      from_cached=False)
    job3 = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['award_procurement'],
                      filename=str(mock_broker_config_paths['d_file_storage_path'].join('diff_end_date')),
                      start_date='01/01/2017', end_date='01/30/2017', original_filename='diff_end_date',
                      from_cached=False)
    sess.add_all([job1, job2, job3])
    sess.commit()

    file_request1 = FileRequestFactory(
        job=job1, is_cached_file=True, agency_code='124', agency_type='awarding', start_date='01/01/2017',
        end_date='01/31/2017', file_type='D1', request_date=datetime.now().date())
    file_request2 = FileRequestFactory(
        job=job1, is_cached_file=True, agency_code='123', agency_type='awarding', start_date='01/02/2017',
        end_date='01/31/2017', file_type='D1', request_date=datetime.now().date())
    file_request3 = FileRequestFactory(
        job=job1, is_cached_file=True, agency_code='123', agency_type='awarding', start_date='01/01/2017',
        end_date='01/30/2017', file_type='D1', request_date=datetime.now().date())
    job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award_procurement'], start_date='01/01/2017', end_date='01/31/2017')
    sess.add_all([job, file_request1, file_request2, file_request3])
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123', 'awarding')

    sess.refresh(job)
    file_request = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id).one_or_none()
    assert file_request is not None
    assert file_request.is_cached_file is True
    assert file_request.start_date == job.start_date
    assert file_request.end_date == job.end_date
    assert file_request.agency_code == '123'
    assert file_request.request_date == datetime.now().date()

    assert job.original_filename != job1.original_filename
    assert job.original_filename != job2.original_filename
    assert job.original_filename != job3.original_filename
    assert job.from_cached is False
    assert job.job_status_id == JOB_STATUS_DICT['finished']


@pytest.mark.usefixtures("job_constants")
def test_generate_noncached_d2_file_success(monkeypatch, mock_broker_config_paths, database):
    """ Testing that a new D2 file is generated """
    sess = database.session
    job1 = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['award_procurement'],
                      filename=str(mock_broker_config_paths['d_file_storage_path'].join('diff_agency')),
                      start_date='01/01/2017', end_date='01/31/2017', original_filename='diff_agency',
                      from_cached=False)
    job2 = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['award'],
                      filename=str(mock_broker_config_paths['d_file_storage_path'].join('diff_start_date')),
                      start_date='01/02/2017', end_date='01/31/2017', original_filename='diff_start_date',
                      from_cached=False)
    job3 = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                      file_type_id=FILE_TYPE_DICT['award_procurement'],
                      filename=str(mock_broker_config_paths['d_file_storage_path'].join('diff_end_date')),
                      start_date='01/01/2017', end_date='01/30/2017', original_filename='diff_end_date',
                      from_cached=False)
    sess.add_all([job1, job2, job3])
    sess.commit()

    file_request1 = FileRequestFactory(
        job=job1, is_cached_file=True, agency_code='124', agency_type='awarding', start_date='01/01/2017',
        end_date='01/31/2017', file_type='D2', request_date=datetime.now().date())
    file_request2 = FileRequestFactory(
        job=job1, is_cached_file=True, agency_code='123', agency_type='awarding', start_date='01/02/2017',
        end_date='01/31/2017', file_type='D2', request_date=datetime.now().date())
    file_request3 = FileRequestFactory(
        job=job1, is_cached_file=True, agency_code='123', agency_type='awarding', start_date='01/01/2017',
        end_date='01/30/2017', file_type='D2', request_date=datetime.now().date())
    job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award_procurement'], start_date='01/01/2017', end_date='01/31/2017')
    sess.add_all([job, file_request1, file_request2, file_request3])
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123', 'awarding')

    sess.refresh(job)
    file_request = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id).one_or_none()
    assert file_request is not None
    assert file_request.is_cached_file is True
    assert file_request.start_date == job.start_date
    assert file_request.end_date == job.end_date
    assert file_request.agency_code == '123'
    assert file_request.request_date == datetime.now().date()

    assert job.original_filename != job1.original_filename
    assert job.original_filename != job2.original_filename
    assert job.original_filename != job3.original_filename
    assert job.from_cached is False
    assert job.job_status_id == JOB_STATUS_DICT['finished']


@pytest.mark.usefixtures("job_constants")
def test_regenerate_same_d1_file_success(monkeypatch, mock_broker_config_paths, database):
    """Testing that a new file is not generated if this job already has a successfully generated file"""
    sess = database.session
    job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award_procurement'],
                     filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
                     start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True)
    sess.add(job)
    sess.commit()

    file_request = FileRequestFactory(
        job=job, is_cached_file=True, agency_code='123', agency_type='awarding', start_date='01/01/2017',
        end_date='01/31/2017', file_type='d1', request_date=datetime.now().date())
    sess.add(file_request)
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123', 'awarding')

    sess.refresh(job)
    file_request = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id).one_or_none()
    assert file_request is not None
    assert file_request.is_cached_file is True
    assert file_request.start_date == job.start_date
    assert file_request.end_date == job.end_date
    assert file_request.agency_code == '123'
    assert file_request.request_date == datetime.now().date()

    assert job.original_filename == 'original'
    assert job.from_cached is False
    assert job.job_status_id == JOB_STATUS_DICT['finished']


@pytest.mark.usefixtures("job_constants")
def test_regenerate_same_d2_file_success(monkeypatch, mock_broker_config_paths, database):
    """Testing that a new file is not generated if this job already has a successfully generated file"""
    sess = database.session
    job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award'],
                     filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
                     start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True)
    sess.add(job)
    sess.commit()

    file_request = FileRequestFactory(
        job=job, is_cached_file=True, agency_code='123', agency_type='awarding', start_date='01/01/2017',
        end_date='01/31/2017', file_type='d2', request_date=datetime.now().date())
    sess.add(file_request)
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123', 'awarding')

    sess.refresh(job)
    file_request = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id).one_or_none()
    assert file_request is not None
    assert file_request.is_cached_file is True
    assert file_request.start_date == job.start_date
    assert file_request.end_date == job.end_date
    assert file_request.agency_code == '123'
    assert file_request.request_date == datetime.now().date()

    assert job.original_filename == 'original'
    assert job.from_cached is False
    assert job.job_status_id == JOB_STATUS_DICT['finished']


@pytest.mark.usefixtures("job_constants")
def test_regenerate_new_d1_file_success(monkeypatch, mock_broker_config_paths, database):
    """Testing that a new file is not generated if another job has already has a successfully generated file"""
    sess = database.session
    original_job = JobFactory(job_status_id=JOB_STATUS_DICT['finished'], job_type_id=JOB_TYPE_DICT['file_upload'],
                              file_type_id=FILE_TYPE_DICT['award_procurement'],
                              filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
                              start_date='01/01/2017', end_date='01/31/2017', original_filename='original',
                              from_cached=True)
    sess.add(original_job)
    sess.commit()

    file_request = FileRequestFactory(
        job=original_job, is_cached_file=True, agency_code='123', agency_type='awarding', start_date='01/01/2017',
        end_date='01/31/2017', file_type='D1', request_date=datetime.now().date())
    new_job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                         file_type_id=FILE_TYPE_DICT['award_procurement'], start_date='01/01/2017',
                         end_date='01/31/2017')
    sess.add_all([file_request, new_job])
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, new_job)))
    FileGenerationManager().generate_from_job(new_job.job_id, '123', 'awarding')

    sess.refresh(new_job)
    file_request = sess.query(FileRequest).filter(FileRequest.job_id == new_job.job_id).one_or_none()
    assert file_request is not None
    assert file_request.is_cached_file is False
    assert file_request.start_date == new_job.start_date
    assert file_request.end_date == new_job.end_date
    assert file_request.agency_code == '123'
    assert file_request.request_date == datetime.now().date()

    assert new_job.original_filename == 'original'
    assert new_job.from_cached is True
    assert new_job.job_status_id == JOB_STATUS_DICT['finished']


@pytest.mark.usefixtures("job_constants")
def test_regenerate_new_d2_file_success(monkeypatch, mock_broker_config_paths, database):
    """Testing that a new file is not generated if another job has already has a successfully generated file"""
    sess = database.session
    original_job = JobFactory(job_status_id=JOB_STATUS_DICT['finished'], job_type_id=JOB_TYPE_DICT['file_upload'],
                              file_type_id=FILE_TYPE_DICT['award'],
                              filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
                              start_date='01/01/2017', end_date='01/31/2017', original_filename='original',
                              from_cached=True)
    sess.add(original_job)
    sess.commit()

    file_request = FileRequestFactory(
        job=original_job, is_cached_file=True, agency_code='123', agency_type='awarding', start_date='01/01/2017',
        end_date='01/31/2017', file_type='D2', request_date=datetime.now().date())
    new_job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                         file_type_id=FILE_TYPE_DICT['award'], start_date='01/01/2017', end_date='01/31/2017')
    sess.add_all([file_request, new_job])
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, new_job)))
    FileGenerationManager().generate_from_job(new_job.job_id, '123', 'awarding')

    sess.refresh(new_job)
    file_request = sess.query(FileRequest).filter(FileRequest.job_id == new_job.job_id).one_or_none()
    assert file_request is not None
    assert file_request.is_cached_file is False
    assert file_request.start_date == new_job.start_date
    assert file_request.end_date == new_job.end_date
    assert file_request.agency_code == '123'
    assert file_request.request_date == datetime.now().date()

    assert new_job.original_filename == 'original'
    assert new_job.from_cached is True
    assert new_job.job_status_id == JOB_STATUS_DICT['finished']


@pytest.mark.usefixtures("job_constants")
def test_uncache_same_d1_file_fpds_success(monkeypatch, mock_broker_config_paths, database):
    """Testing that a new file is not generated if this job already has a successfully generated file"""
    sess = database.session
    job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award_procurement'],
                     filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
                     start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True)
    sess.add(job)
    sess.commit()

    file_request = FileRequestFactory(
        job=job, is_cached_file=True, agency_code='123', agency_type='awarding', start_date='01/01/2017',
        end_date='01/31/2017', file_type='d1', request_date=(datetime.now().date() - timedelta(1)))
    sess.add(file_request)
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123', 'awarding')

    sess.refresh(job)
    file_request = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id).one_or_none()
    assert file_request is not None
    assert file_request.is_cached_file is True
    assert file_request.start_date == job.start_date
    assert file_request.end_date == job.end_date
    assert file_request.agency_code == '123'
    assert file_request.request_date == datetime.now().date()

    assert job.original_filename != 'original'
    assert job.from_cached is False
    assert job.job_status_id == JOB_STATUS_DICT['finished']


@pytest.mark.usefixtures("job_constants")
def test_uncache_new_d1_file_fpds_success(monkeypatch, mock_broker_config_paths, database):
    """Testing that a new file is not generated if another job has already has a successfully generated file"""
    sess = database.session
    original_job = JobFactory(job_status_id=JOB_STATUS_DICT['finished'], job_type_id=JOB_TYPE_DICT['file_upload'],
                              file_type_id=FILE_TYPE_DICT['award_procurement'],
                              filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
                              start_date='01/01/2017', end_date='01/31/2017', original_filename='original',
                              from_cached=True)
    sess.add(original_job)
    sess.commit()

    file_request = FileRequestFactory(
        job=original_job, is_cached_file=True, agency_code='123', agency_type='awarding', start_date='01/01/2017',
        end_date='01/31/2017', file_type='D1', request_date=(datetime.now().date() - timedelta(1)))
    new_job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                         file_type_id=FILE_TYPE_DICT['award_procurement'], start_date='01/01/2017',
                         end_date='01/31/2017')
    sess.add_all([file_request, new_job])
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, new_job)))
    FileGenerationManager().generate_from_job(new_job.job_id, '123', 'awarding')

    sess.refresh(new_job)
    file_request = sess.query(FileRequest).filter(FileRequest.job_id == new_job.job_id).one_or_none()
    assert file_request is not None
    assert file_request.is_cached_file is True
    assert file_request.start_date == new_job.start_date
    assert file_request.end_date == new_job.end_date
    assert file_request.agency_code == '123'
    assert file_request.request_date == datetime.now().date()

    assert new_job.original_filename != 'original'
    assert new_job.from_cached is False
    assert new_job.job_status_id == JOB_STATUS_DICT['finished']


@pytest.mark.usefixtures("job_constants")
def test_generate_new_d1_file_funding_with_awarding_success(monkeypatch, mock_broker_config_paths, database):
    """ Testing that a new D1 file is generated using funding agency if there's a FileRequest for awarding agency """
    sess = database.session
    job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award_procurement'],
                     filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
                     start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True)
    sess.add(job)
    sess.commit()

    fr = FileRequestFactory(
        job=job, is_cached_file=True, agency_code='123', agency_type='awarding', start_date=job.start_date,
        end_date=job.end_date, file_type='D1', request_date=datetime.now().date())
    sess.add(fr)
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123', 'funding')

    sess.refresh(job)
    new_file_request = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id,
                                                      FileRequest.is_cached_file.is_(True)).one_or_none()
    assert new_file_request is not None
    assert new_file_request.is_cached_file is True
    assert new_file_request.start_date == job.start_date
    assert new_file_request.end_date == job.end_date
    assert new_file_request.agency_code == '123'
    assert new_file_request.agency_type == 'funding'

    old_file_request = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id,
                                                      FileRequest.is_cached_file.is_(False)).one_or_none()
    assert old_file_request is not None
    assert old_file_request.is_cached_file is False
    assert old_file_request.start_date == job.start_date
    assert old_file_request.end_date == job.end_date
    assert old_file_request.agency_code == '123'
    assert old_file_request.agency_type == 'awarding'


@pytest.mark.usefixtures("job_constants")
def test_generate_new_d1_file_different_dates_success(monkeypatch, mock_broker_config_paths, database):
    """ Testing that a new D1 file is generated using the same data except for the dates """
    sess = database.session
    job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                     file_type_id=FILE_TYPE_DICT['award_procurement'],
                     filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
                     start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=True)
    sess.add(job)
    sess.commit()

    fr = FileRequestFactory(
        job=job, is_cached_file=True, agency_code='123', agency_type='awarding', start_date=job.start_date,
        end_date=job.end_date, file_type='D1', request_date=datetime.now().date())
    sess.add(fr)
    sess.commit()

    # Change the job start date
    old_start_date = job.start_date
    job.start_date = '01/02/2017'
    sess.commit()

    monkeypatch.setattr(file_generation_handler, 'retrieve_job_context_data', Mock(return_value=(sess, job)))
    FileGenerationManager().generate_from_job(job.job_id, '123', 'awarding')

    sess.refresh(job)
    new_file_request = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id,
                                                      FileRequest.is_cached_file.is_(True)).one_or_none()
    assert new_file_request is not None
    assert new_file_request.is_cached_file is True
    assert new_file_request.start_date == job.start_date
    assert new_file_request.end_date == job.end_date
    assert new_file_request.agency_code == '123'
    assert new_file_request.agency_type == 'awarding'

    old_file_request = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id,
                                                      FileRequest.is_cached_file.is_(False)).one_or_none()
    assert old_file_request is not None
    assert old_file_request.is_cached_file is False
    assert old_file_request.start_date == old_start_date
    assert old_file_request.end_date == job.end_date
    assert old_file_request.agency_code == '123'
    assert old_file_request.agency_type == 'awarding'
