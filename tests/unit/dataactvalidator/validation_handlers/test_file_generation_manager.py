import csv
import os
import re
import pytest

from collections import OrderedDict
from datetime import datetime, timedelta
from unittest.mock import Mock

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.models.jobModels import FileRequest
from dataactcore.models.stagingModels import DetachedAwardProcurement, PublishedAwardFinancialAssistance
from dataactcore.utils import fileE

from dataactvalidator.validation_handlers import file_generation_manager
from dataactvalidator.validation_handlers.file_generation_manager import FileGenerationManager

from tests.unit.dataactcore.factories.job import JobFactory, FileRequestFactory, SubmissionFactory
from tests.unit.dataactcore.factories.staging import (
    AwardFinancialAssistanceFactory, AwardProcurementFactory, DetachedAwardProcurementFactory,
    PublishedAwardFinancialAssistanceFactory)


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

    file_gen_manager = FileGenerationManager(job, '123', 'awarding', CONFIG_BROKER['local'])
    file_gen_manager.generate_from_job()

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

    file_gen_manager = FileGenerationManager(job, '123', 'funding', CONFIG_BROKER['local'])
    file_gen_manager.generate_from_job()

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

    file_gen_manager = FileGenerationManager(job, '123', 'awarding', CONFIG_BROKER['local'])
    file_gen_manager.generate_from_job()

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

    file_gen_manager = FileGenerationManager(job, '123', 'awarding', CONFIG_BROKER['local'])
    file_gen_manager.generate_from_job()

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

    file_gen_manager = FileGenerationManager(job, '123', 'awarding', CONFIG_BROKER['local'])
    file_gen_manager.generate_from_job()

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

    file_gen_manager = FileGenerationManager(job, '123', 'awarding', CONFIG_BROKER['local'])
    file_gen_manager.generate_from_job()

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

    file_gen_manager = FileGenerationManager(job, '123', 'awarding', CONFIG_BROKER['local'])
    file_gen_manager.generate_from_job()

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

    file_gen_manager = FileGenerationManager(new_job, '123', 'awarding', CONFIG_BROKER['local'])
    file_gen_manager.generate_from_job()

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

    file_gen_manager = FileGenerationManager(new_job, '123', 'awarding', CONFIG_BROKER['local'])
    file_gen_manager.generate_from_job()

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
                     start_date='01/01/2017', end_date='01/31/2017', original_filename='original', from_cached=False)
    sess.add(job)
    sess.commit()

    gen_date = datetime.now().date() - timedelta(1)
    file_request = FileRequestFactory(
        job=job, is_cached_file=True, agency_code='123', agency_type='awarding', start_date='01/01/2017',
        end_date='01/31/2017', file_type='d1', request_date=gen_date)
    sess.add(file_request)
    sess.commit()

    file_gen_manager = FileGenerationManager(job, '123', 'awarding', CONFIG_BROKER['local'])
    file_gen_manager.generate_from_job()

    sess.refresh(job)
    file_request = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id).one_or_none()
    assert file_request is not None
    assert file_request.is_cached_file is True
    assert file_request.start_date == job.start_date
    assert file_request.end_date == job.end_date
    assert file_request.agency_code == '123'
    assert file_request.request_date == gen_date

    assert job.original_filename == 'original'
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

    file_gen_manager = FileGenerationManager(new_job, '123', 'awarding', CONFIG_BROKER['local'])
    file_gen_manager.generate_from_job()

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

    file_gen_manager = FileGenerationManager(job, '123', 'funding', CONFIG_BROKER['local'])
    file_gen_manager.generate_from_job()

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

    file_gen_manager = FileGenerationManager(job, '123', 'awarding', CONFIG_BROKER['local'])
    file_gen_manager.generate_from_job()

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


@pytest.mark.usefixtures("job_constants")
def test_generate_new_d1_file_keep_old_job_files_success(monkeypatch, mock_broker_config_paths, database):
    """ Testing that when a new file is generated by a child job, the parent job's files stay the same """
    sess = database.session
    original_job = JobFactory(job_status_id=JOB_STATUS_DICT['waiting'], job_type_id=JOB_TYPE_DICT['file_upload'],
                              file_type_id=FILE_TYPE_DICT['award_procurement'],
                              filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
                              start_date='01/01/2017', end_date='01/31/2017', original_filename='original',
                              from_cached=False)
    new_job = JobFactory(job_status_id=JOB_STATUS_DICT['finished'], job_type_id=JOB_TYPE_DICT['file_upload'],
                         file_type_id=FILE_TYPE_DICT['award_procurement'],
                         filename=str(mock_broker_config_paths['d_file_storage_path'].join('original')),
                         start_date='01/01/2017', end_date='01/31/2017', original_filename='original',
                         from_cached=False)
    sess.add_all([original_job, new_job])
    sess.commit()

    fr = FileRequestFactory(
        job=original_job, parent_job_id=None, is_cached_file=False, agency_code='123', agency_type='awarding',
        start_date=original_job.start_date, end_date=original_job.end_date, file_type='D1',
        request_date=datetime.now().date())
    fr_2 = FileRequestFactory(
        job=new_job, parent_job_id=original_job.job_id, is_cached_file=False, agency_code='123', agency_type='awarding',
        start_date=new_job.start_date, end_date=new_job.end_date, file_type='D1', request_date=datetime.now().date())
    sess.add_all([fr, fr_2])
    sess.commit()

    file_gen_manager = FileGenerationManager(original_job, '123', 'awarding', CONFIG_BROKER['local'])
    file_gen_manager.generate_from_job()

    sess.refresh(original_job)
    sess.refresh(new_job)

    assert original_job.original_filename != 'original'

    assert new_job.original_filename == 'original'


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

    file_gen_manager = FileGenerationManager(job, '123', 'awarding', CONFIG_BROKER['local'])
    file_gen_manager.generate_d_file()

    # check headers
    file_rows = read_file_rows(file_path)
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

    file_gen_manager = FileGenerationManager(job, '123', 'awarding', CONFIG_BROKER['local'])
    file_gen_manager.generate_d_file()

    # check headers
    file_rows = read_file_rows(file_path)
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
    monkeypatch.setattr(file_generation_manager, 'fileF', file_f_mock)
    file_f_mock.generate_f_rows.return_value = [dict(key4='a', key11='b'), dict(key4='c', key11='d')]
    file_f_mock.mappings = OrderedDict([('key4', 'mapping4'), ('key11', 'mapping11')])
    expected = [['key4', 'key11'], ['a', 'b'], ['c', 'd']]

    monkeypatch.setattr(file_generation_manager, 'mark_job_status', Mock())

    file_gen_manager = FileGenerationManager(job1, None, None, CONFIG_BROKER['local'])
    file_gen_manager.generate_f_file()

    assert read_file_rows(file_path1) == expected

    # re-order
    file_f_mock.mappings = OrderedDict([('key11', 'mapping11'), ('key4', 'mapping4')])
    expected = [['key11', 'key4'], ['b', 'a'], ['d', 'c']]

    monkeypatch.setattr(file_generation_manager, 'mark_job_status', Mock())

    file_gen_manager = FileGenerationManager(job2, None, None, CONFIG_BROKER['local'])
    file_gen_manager.generate_f_file()

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

    monkeypatch.setattr(file_generation_manager, 'mark_job_status', Mock())
    monkeypatch.setattr(file_generation_manager.fileE, 'retrieve_rows', Mock(return_value=[]))

    file_gen_manager = FileGenerationManager(job, None, None, CONFIG_BROKER['local'])
    file_gen_manager.generate_e_file()

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

    monkeypatch.setattr(file_generation_manager, 'mark_job_status', Mock())

    file_gen_manager = FileGenerationManager(job, None, None, CONFIG_BROKER['local'])
    file_gen_manager.generate_e_file()

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


def read_file_rows(file_path):
    assert os.path.isfile(file_path)

    with open(file_path) as f:
        return [row for row in csv.reader(f)]
