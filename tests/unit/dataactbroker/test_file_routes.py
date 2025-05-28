import json
import os
from unittest.mock import Mock

from flask import g
import pytest

from dataactbroker.routes import file_routes
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory, BannerFactory, ApplicationTypeFactory
from tests.unit.dataactcore.factories.user import UserFactory
from datetime import datetime, timedelta


@pytest.fixture
def file_app(test_app):
    file_routes.add_file_routes(test_app.application, Mock(), Mock())
    yield test_app


def test_list_banners(file_app, database):
    """Test listing the public broker banners (which to show/hide based on the banner)"""
    fabs_app = ApplicationTypeFactory(application_name="fabs", application_type_id="1")
    dabs_app = ApplicationTypeFactory(application_name="dabs", application_type_id="2")
    all_app = ApplicationTypeFactory(application_name="all", application_type_id="3")
    login_app = ApplicationTypeFactory(application_name="login", application_type_id="4")
    database.session.add_all([fabs_app, dabs_app, all_app, login_app])

    # This first one should be hidden due to old dates
    first_old = BannerFactory(
        start_date=datetime(2007, 1, 3),
        end_date=datetime(2010, 3, 5),
        block_certification=False,
        message="first",
        application_type=fabs_app,
    )
    database.session.add(first_old)

    response = file_app.get("/v1/list_banners/")
    response_json = json.loads(response.data.decode("UTF-8"))
    assert response_json["data"] is None

    # This one should appear with the appropriate dates
    curr_date = datetime.now()
    diff = timedelta(days=1)
    second_current = BannerFactory(
        start_date=curr_date - diff,
        end_date=curr_date + diff,
        block_certification=False,
        message="second",
        application_type=fabs_app,
        banner_type="info",
    )
    database.session.add(second_current)

    response = file_app.get("/v1/list_banners/")
    response_json = json.loads(response.data.decode("UTF-8"))
    second_response = {
        "start_date": str(curr_date - diff),
        "end_date": str(curr_date + diff),
        "header": None,
        "message": "second",
        "type": fabs_app.application_name,
        "banner_type": "info",
        "notice_block": False,
    }
    assert response_json["data"][0] == second_response

    # Another example with different values that should be appear.
    # The fourth login banner shouldn't appear since the endpoint defauts to ignoring login banners
    curr_date = datetime.now()
    diff = timedelta(days=1)
    third_curent = BannerFactory(
        start_date=curr_date - diff,
        end_date=curr_date + diff,
        block_certification=True,
        message="third",
        application_type=dabs_app,
        banner_type="warning",
    )
    fourth_current = BannerFactory(
        start_date=curr_date - diff,
        end_date=curr_date + diff,
        block_certification=True,
        message="fourth",
        application_type=login_app,
        banner_type="info",
        header="FOURTH",
    )
    database.session.add_all([third_curent, fourth_current])

    response = file_app.get("/v1/list_banners/")
    response_json = json.loads(response.data.decode("UTF-8"))
    third_response = {
        "start_date": str(curr_date - diff),
        "end_date": str(curr_date + diff),
        "header": None,
        "message": "third",
        "type": dabs_app.application_name,
        "banner_type": "warning",
        "notice_block": True,
    }
    sort_key = lambda k: k["message"]
    assert sorted(response_json["data"], key=sort_key) == sorted([second_response, third_response], key=sort_key)

    # Double checking the login banner only gets shown when the login param is true
    response = file_app.get("/v1/list_banners/?login=true")
    response_json = json.loads(response.data.decode("UTF-8"))
    fourth_response = {
        "start_date": str(curr_date - diff),
        "end_date": str(curr_date + diff),
        "header": "FOURTH",
        "message": "fourth",
        "type": login_app.application_name,
        "banner_type": "info",
        "notice_block": True,
    }
    assert response_json["data"][0] == fourth_response


@pytest.mark.usefixtures("user_constants")
@pytest.mark.usefixtures("job_constants")
def test_current_page(file_app, database):
    """Test the route to check what the current progress of the submission is at
    the correct page
    """

    cgac = CGACFactory()
    user = UserFactory.with_cgacs(cgac)
    user.user_id = 1
    user.name = "Oliver Queen"
    user.website_admin = True
    database.session.add(user)
    database.session.commit()
    g.user = user

    sub = SubmissionFactory(user_id=1, cgac_code=cgac.cgac_code)
    database.session.add(sub)
    database.session.commit()

    csv_validation = JOB_TYPE_DICT["csv_record_validation"]
    upload = JOB_TYPE_DICT["file_upload"]
    validation = JOB_TYPE_DICT["validation"]
    finished_job = JOB_STATUS_DICT["finished"]
    waiting = JOB_STATUS_DICT["waiting"]

    job_a_u = JobFactory(
        submission=sub,
        file_type_id=FILE_TYPE_DICT["appropriations"],
        job_type_id=upload,
        number_of_errors=0,
        file_size=123,
        job_status_id=finished_job,
        filename="A.csv",
        original_filename="A.csv",
    )
    job_a_v = JobFactory(
        submission=sub,
        file_type_id=FILE_TYPE_DICT["appropriations"],
        job_type_id=csv_validation,
        number_of_errors=0,
        file_size=123,
        job_status_id=finished_job,
        filename="A.csv",
        original_filename="A.csv",
    )
    job_b_u = JobFactory(
        submission=sub,
        file_type_id=FILE_TYPE_DICT["program_activity"],
        job_type_id=upload,
        number_of_errors=0,
        file_size=123,
        job_status_id=finished_job,
        filename="B.csv",
        original_filename="B.csv",
    )
    job_b_v = JobFactory(
        submission=sub,
        file_type_id=FILE_TYPE_DICT["program_activity"],
        job_type_id=csv_validation,
        number_of_errors=0,
        file_size=123,
        job_status_id=finished_job,
        filename="B.csv",
        original_filename="B.csv",
    )
    job_c_u = JobFactory(
        submission=sub,
        file_type_id=FILE_TYPE_DICT["award_financial"],
        job_type_id=upload,
        number_of_errors=0,
        file_size=123,
        job_status_id=finished_job,
        filename="C.csv",
        original_filename="C.csv",
    )
    job_c_v = JobFactory(
        submission=sub,
        file_type_id=FILE_TYPE_DICT["award_financial"],
        job_type_id=csv_validation,
        number_of_errors=0,
        file_size=123,
        job_status_id=finished_job,
        filename="C.csv",
        original_filename="C.csv",
    )
    job_d1_u = JobFactory(
        submission=sub,
        file_type_id=FILE_TYPE_DICT["award_procurement"],
        job_type_id=upload,
        number_of_errors=0,
        file_size=123,
        job_status_id=finished_job,
        filename="D1.csv",
        original_filename="D1.csv",
    )
    job_d1_v = JobFactory(
        submission=sub,
        file_type_id=FILE_TYPE_DICT["award_procurement"],
        job_type_id=csv_validation,
        number_of_errors=0,
        file_size=123,
        job_status_id=finished_job,
        filename="D1.csv",
        original_filename="D1.csv",
    )
    job_d2_u = JobFactory(
        submission=sub,
        file_type_id=FILE_TYPE_DICT["award"],
        job_type_id=upload,
        number_of_errors=0,
        file_size=123,
        job_status_id=finished_job,
        filename="D2.csv",
        original_filename="D2.csv",
    )
    job_d2_v = JobFactory(
        submission=sub,
        file_type_id=FILE_TYPE_DICT["award"],
        job_type_id=csv_validation,
        number_of_errors=0,
        file_size=123,
        job_status_id=finished_job,
        filename="D2.csv",
        original_filename="D2.csv",
    )
    job_e = JobFactory(
        submission=sub,
        file_type_id=FILE_TYPE_DICT["executive_compensation"],
        job_type_id=upload,
        number_of_errors=0,
        file_size=123,
        job_status_id=finished_job,
        filename="E.csv",
    )
    job_f = JobFactory(
        submission=sub,
        file_type_id=FILE_TYPE_DICT["sub_award"],
        job_type_id=upload,
        number_of_errors=0,
        file_size=123,
        job_status_id=finished_job,
        filename="F.csv",
    )
    job_cross_file = JobFactory(
        submission=sub,
        file_type_id=None,
        job_type_id=validation,
        number_of_errors=0,
        file_size=123,
        job_status_id=finished_job,
    )

    database.session.add_all(
        [
            job_a_u,
            job_a_v,
            job_b_u,
            job_b_v,
            job_c_u,
            job_c_v,
            job_d1_u,
            job_d1_v,
            job_d2_u,
            job_d2_v,
            job_e,
            job_f,
            job_cross_file,
        ]
    )
    database.session.commit()

    # Everything ok
    response = file_app.get(f"/v1/check_current_page/?submission_id={sub.submission_id}")
    response_json = json.loads(response.data.decode("UTF-8"))
    assert response_json["step"] == "5"

    # Check for file URL
    response = file_app.get(f"/v1/get_file_url/?submission_id={sub.submission_id}&file_type=A")
    response_json = json.loads(response.data.decode("UTF-8"))
    assert os.path.basename(response_json["url"]) == "A.csv"

    job_e.job_status_id = 6
    database.session.commit()
    # E or F failed
    response = file_app.get(f"/v1/check_current_page/?submission_id={sub.submission_id}")
    response_json = json.loads(response.data.decode("UTF-8"))
    assert response_json["step"] == "4"

    job_e.job_status_id = 4
    job_cross_file.number_of_errors = 6
    database.session.commit()

    # Restore job_e and create errors for cross_file
    response = file_app.get(f"/v1/check_current_page/?submission_id={sub.submission_id}")
    response_json = json.loads(response.data.decode("UTF-8"))
    assert response_json["step"] == "3"

    job_d1_v.number_of_errors = 6
    database.session.commit()
    # D file has errors
    response = file_app.get(f"/v1/check_current_page/?submission_id={sub.submission_id}")
    response_json = json.loads(response.data.decode("UTF-8"))
    assert response_json["step"] == "2"

    job_c_v.number_of_errors = 6
    database.session.commit()
    # Fail C file validation
    response = file_app.get(f"/v1/check_current_page/?submission_id={sub.submission_id}")
    response_json = json.loads(response.data.decode("UTF-8"))
    assert response_json["step"] == "1"

    job_cross_file.job_status_id = waiting
    job_d1_v.number_of_errors = 0
    database.session.commit()
    # E and F generated with C file errors
    response = file_app.get(f"/v1/check_current_page/?submission_id={sub.submission_id}")
    response_json = json.loads(response.data.decode("UTF-8"))
    assert response_json["step"] == "1"
