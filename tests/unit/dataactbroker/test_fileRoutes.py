import json
from unittest.mock import Mock

from flask import g
import pytest

from dataactbroker import fileRoutes
from dataactcore.models.lookups import PUBLISH_STATUS_DICT
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory, WindowFactory
from tests.unit.dataactcore.factories.user import UserFactory
from dataactcore.models.jobModels import JobStatus, JobType, FileType
from datetime import datetime, timedelta


@pytest.fixture
def file_app(test_app):
    fileRoutes.add_file_routes(test_app.application, Mock(), Mock(), Mock())
    yield test_app


def sub_ids(response):
    """Helper function to parse out the submission ids from an HTTP
    response"""
    assert response.status_code == 200
    result = json.loads(response.data.decode('UTF-8'))
    assert 'submissions' in result
    return {sub['submission_id'] for sub in result['submissions']}


def test_list_submissions(file_app, database, user_constants, job_constants):
    """Test listing user's submissions. The expected values here correspond to
    the number of submissions within the agency of the user that is logged in
    """
    cgacs = [CGACFactory() for _ in range(5)]
    user1 = UserFactory.with_cgacs(cgacs[0], cgacs[1])
    user2 = UserFactory.with_cgacs(cgacs[2])
    user3 = UserFactory.with_cgacs(*cgacs)
    database.session.add_all(cgacs + [user1, user2, user3])
    database.session.commit()

    submissions = [     # one submission per CGAC
        SubmissionFactory(cgac_code=cgac.cgac_code, publish_status_id=PUBLISH_STATUS_DICT['unpublished'])
        for cgac in cgacs
    ]
    database.session.add_all(submissions)

    g.user = user1
    response = file_app.get("/v1/list_submissions/?certified=mixed")
    assert sub_ids(response) == {sub.submission_id for sub in submissions[:2]}

    response = file_app.get("/v1/list_submissions/?certified=false")
    assert sub_ids(response) == {sub.submission_id for sub in submissions[:2]}

    response = file_app.get("/v1/list_submissions/?certified=true")
    assert sub_ids(response) == set()

    submissions[0].publish_status_id = PUBLISH_STATUS_DICT['published']
    database.session.commit()
    response = file_app.get("/v1/list_submissions/?certified=true")
    assert sub_ids(response) == {submissions[0].submission_id}

    g.user = user2
    response = file_app.get("/v1/list_submissions/?certified=mixed")
    assert sub_ids(response) == {submissions[2].submission_id}

    g.user = user3
    submissions[3].d2_submission = True
    submissions[4].d2_submission = True
    database.session.commit()
    response = file_app.get("/v1/list_submissions/?certified=mixed&d2_submission=true")
    assert sub_ids(response) == {sub.submission_id for sub in submissions[3:]}

    response = file_app.get("/v1/list_submissions/?certified=mixed")
    assert sub_ids(response) == {sub.submission_id for sub in submissions[:3]}


def test_is_period(file_app, database):
    """Test listing user's submissions. The expected values here correspond to
    the number of submissions within the agency of the user that is logged in
    """

    gtas = WindowFactory(start_date=datetime(2007, 1, 3), end_date=datetime(2010, 3, 5), block_certification=False)
    database.session.add(gtas)

    response = file_app.get("/v1/window/")
    response_json = json.loads(response.data.decode('UTF-8'))
    assert response_json['data'] is None

    gtas = WindowFactory(start_date=datetime(2007, 1, 3), end_date=datetime(2010, 3, 5), block_certification=True)
    database.session.add(gtas)

    response = file_app.get("/v1/window/")
    response_json = json.loads(response.data.decode('UTF-8'))
    assert response_json['data'] is None

    curr_date = datetime.now()
    diff = timedelta(days=1)
    gtas_current = WindowFactory(start_date=curr_date-diff, end_date=curr_date+diff, block_certification=False)
    database.session.add(gtas_current)

    response = file_app.get("/v1/window/")
    response_json = json.loads(response.data.decode('UTF-8'))
    assert response_json['data'][0]['notice_block'] is False

    curr_date = datetime.now()
    diff = timedelta(days=1)
    gtas_current = WindowFactory(start_date=curr_date-diff, end_date=curr_date+diff, block_certification=True)
    database.session.add(gtas_current)

    response = file_app.get("/v1/window/")
    response_json = json.loads(response.data.decode('UTF-8'))
    assert response_json['data'][1]['notice_block'] is True


def test_current_page(file_app, database, user_constants, job_constants, monkeypatch):
    """Test the route to check what the current progress of the submission is at
    the correct page
    """

    cgac = CGACFactory()
    user = UserFactory.with_cgacs(cgac)
    user.user_id = 1
    user.name = 'Oliver Queen'
    user.website_admin = True
    database.session.add(user)
    database.session.commit()
    g.user = user

    sub = SubmissionFactory(user_id=1, cgac_code=cgac.cgac_code)
    database.session.add(sub)

    csv_validation = database.session.query(JobType).filter_by(name='csv_record_validation').one()
    upload = database.session.query(JobType).filter_by(name='file_upload').one()
    validation = database.session.query(JobType).filter_by(name='validation').one()
    finished_job = database.session.query(JobStatus).filter_by(name='finished').one()
    waiting = database.session.query(JobStatus).filter_by(name='waiting').one()

    job_a = JobFactory(submission_id=sub.submission_id, file_type=database.session.query(FileType)
                       .filter_by(name='appropriations').one(), job_type=csv_validation, number_of_errors=0,
                       file_size=123, job_status=finished_job)
    job_b = JobFactory(submission_id=sub.submission_id, file_type=database.session.query(FileType)
                       .filter_by(name='program_activity').one(), job_type=csv_validation, number_of_errors=0,
                       file_size=123, job_status=finished_job)
    job_c = JobFactory(submission_id=sub.submission_id, file_type=database.session.query(FileType)
                       .filter_by(name='award_financial').one(), job_type=csv_validation, number_of_errors=0,
                       file_size=123, job_status=finished_job)
    job_d1 = JobFactory(submission_id=sub.submission_id, file_type=database.session.query(FileType)
                        .filter_by(name='award_procurement').one(), job_type=csv_validation, number_of_errors=0,
                        file_size=123, job_status=finished_job)
    job_d2 = JobFactory(submission_id=sub.submission_id, file_type=database.session.query(FileType)
                        .filter_by(name='award').one(), job_type=csv_validation, number_of_errors=0, file_size=123,
                        job_status=finished_job)
    job_e = JobFactory(submission_id=sub.submission_id, file_type=database.session.query(FileType)
                       .filter_by(name='executive_compensation').one(), job_type=upload, number_of_errors=0,
                       file_size=123, job_status=finished_job)
    job_f = JobFactory(submission_id=sub.submission_id, file_type=database.session.query(FileType)
                       .filter_by(name='sub_award').one(), job_type=upload, number_of_errors=0, file_size=123,
                       job_status=finished_job)
    job_cross_file = JobFactory(submission_id=sub.submission_id, file_type=None, job_type=validation,
                                number_of_errors=0, file_size=123, job_status=finished_job)

    database.session.add_all([job_a, job_b, job_c, job_d1, job_d2, job_e, job_f, job_cross_file])
    database.session.commit()

    # Everything ok
    response = file_app.get("/v1/check_current_page/?submission_id=" + str(sub.submission_id))
    response_json = json.loads(response.data.decode('UTF-8'))
    assert response_json['step'] == '5'

    job_e.job_status_id = 6
    database.session.commit()
    # E or F failed
    response = file_app.get("/v1/check_current_page/?submission_id=" + str(sub.submission_id))
    response_json = json.loads(response.data.decode('UTF-8'))
    assert response_json['step'] == '4'

    job_e.job_status_id = 4
    job_cross_file.number_of_errors = 6
    database.session.commit()

    # Restore job_e and create errors for cross_file
    response = file_app.get("/v1/check_current_page/?submission_id=" + str(sub.submission_id))
    response_json = json.loads(response.data.decode('UTF-8'))
    assert response_json['step'] == '3'

    job_d1.number_of_errors = 6
    database.session.commit()
    # D file has errors
    response = file_app.get("/v1/check_current_page/?submission_id=" + str(sub.submission_id))
    response_json = json.loads(response.data.decode('UTF-8'))
    assert response_json['step'] == '2'

    job_c.number_of_errors = 6
    database.session.commit()
    # Fail C file validation
    response = file_app.get("/v1/check_current_page/?submission_id=" + str(sub.submission_id))
    response_json = json.loads(response.data.decode('UTF-8'))
    assert response_json['step'] == '1'

    job_cross_file.job_status = waiting
    job_d1.number_of_errors = 0
    database.session.commit()
    # E and F generated with C file errors
    response = file_app.get("/v1/check_current_page/?submission_id=" + str(sub.submission_id))
    response_json = json.loads(response.data.decode('UTF-8'))
    assert response_json['step'] == '1'
