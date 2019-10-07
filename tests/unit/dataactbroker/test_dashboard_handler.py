import json
import pytest
from datetime import datetime
from unittest.mock import Mock

from tests.unit.dataactcore.factories.user import UserFactory
from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from dataactcore.models.userModel import UserAffiliation
from dataactcore.models.lookups import PERMISSION_TYPE_DICT, PUBLISH_STATUS_DICT
from dataactbroker.helpers.generic_helper import fy
from dataactbroker.handlers import dashboard_handler
from dataactcore.utils.responseException import ResponseException


def historic_dabs_warning_summary_endpoint(filters):
    json_response = dashboard_handler.historic_dabs_warning_summary(filters)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def test_validate_historic_dashboard_filters():
    def assert_validation(filters, expected_response):
        with pytest.raises(ResponseException) as resp_except:
            dashboard_handler.validate_historic_dashboard_filters(filters)

        assert resp_except.value.status == 500
        assert str(resp_except.value) == expected_response

    # missing a required filter
    assert_validation({'quarters': [], 'fys': []}, 'The following filters were not provided: agencies')

    # not a list
    filters = {'quarters': [1], 'fys': 'not a list', 'agencies': ['097']}
    assert_validation(filters, 'The following filters were not lists: fys')

    # wrong quarters
    error_message = 'Quarters must be a list of integers, each ranging 1-4, or an empty list.'
    filters = {'quarters': [5], 'fys': [2017, 2019], 'agencies': ['097']}
    assert_validation(filters, error_message)
    filters['quarters'] = ['3']
    assert_validation(filters, error_message)

    # wrong fys
    current_fy = fy(datetime.now())
    error_message = 'Fiscal Years must be a list of integers, each ranging from 2017 through the current fiscal year,'\
                    ' or an empty list.'
    filters = {'quarters': [1, 3], 'fys': [2016, 2019], 'agencies': ['097']}
    assert_validation(filters, error_message)
    filters = {'quarters': [1, 3], 'fys': [2017, current_fy+1], 'agencies': ['097']}
    assert_validation(filters, error_message)
    filters = {'quarters': [1, 3], 'fys': [2017, str(current_fy)], 'agencies': ['097']}
    assert_validation(filters, error_message)

    # wrong agencies
    filters = {'quarters': [1, 3], 'fys': [2017, 2019], 'agencies': [97]}
    assert_validation(filters, 'Agencies must be a list of strings, or an empty list.')


def sort_results(results):
    return sorted(results, key=lambda d: d['submission_id'])


def setup_submissions(sess, admin=False):
    db_objects = []

    # Setup agencies
    cgac1 = CGACFactory(cgac_code='089', agency_name='CGAC')
    cgac2 = CGACFactory(cgac_code='011', agency_name='CGAC Associated with FREC')
    cgac3 = CGACFactory(cgac_code='091', agency_name='Other CGAC')
    frec = FRECFactory(cgac=cgac2, frec_code='1125', agency_name='FREC')
    db_objects.extend([cgac1, cgac2, cgac3, frec])

    # Setup users and affiliations
    agency_user = UserFactory(name='Agency User', affiliations=[
        UserAffiliation(user_affiliation_id=1, cgac=cgac1, permission_type_id=PERMISSION_TYPE_DICT['reader'])
    ])
    admin_user = UserFactory(name='Administrator', website_admin=True)
    db_objects.extend([agency_user, admin_user])

    # Setup submissions
    sub1 = SubmissionFactory(submission_id=1, reporting_fiscal_period=9, reporting_fiscal_year=2017,
                             certifying_user_id=agency_user.user_id, cgac_code=cgac1.cgac_code, frec_code=None,
                             publish_status_id=PUBLISH_STATUS_DICT['updated'], d2_submission=False,
                             user_id=agency_user.user_id)
    sub2 = SubmissionFactory(submission_id=2, reporting_fiscal_period=3, reporting_fiscal_year=2019,
                             certifying_user_id=admin_user.user_id, cgac_code=cgac2.cgac_code,
                             frec_code=frec.frec_code, publish_status_id=PUBLISH_STATUS_DICT['published'],
                             d2_submission=False, user_id=admin_user.user_id)
    sub3 = SubmissionFactory(submission_id=3, reporting_fiscal_period=3, reporting_fiscal_year=2019,
                             certifying_user_id=agency_user.user_id, cgac_code=cgac3.cgac_code,
                             frec_code=None, publish_status_id=PUBLISH_STATUS_DICT['published'],
                             d2_submission=False, user_id=agency_user.user_id)
    db_objects.extend([sub1, sub2, sub3])

    sess.add_all(db_objects)
    sess.commit()

    user = agency_user if not admin else admin_user
    return user


@pytest.mark.usefixtures("job_constants")
@pytest.mark.usefixtures("user_constants")
def test_historic_dabs_warning_summary_admin(database, monkeypatch):
    sess = database.session

    user = setup_submissions(sess, admin=True)
    monkeypatch.setattr(dashboard_handler, 'g', Mock(user=user))

    # Responses
    sub1_response = {
        "submission_id": 1,
        "fy": 2017,
        "certifier": "Agency User",
        "quarter": 3,
        "agency": {
            "name": "CGAC",
            "code": "089"
        }
    }
    sub2_response = {
        "submission_id": 2,
        "fy": 2019,
        "certifier": "Administrator",
        "quarter": 1,
        "agency": {
            "name": "FREC",
            "code": "1125"
        }
    }
    sub3_response = {
        "submission_id": 3,
        "fy": 2019,
        "certifier": "Agency User",
        "quarter": 1,
        "agency": {
            "name": "Other CGAC",
            "code": "091"
        }
    }

    # Perfect case
    filters = {
        'quarters': [1, 3],
        'fys': [2017, 2019],
        'agencies': ['089', '1125', '091']
    }
    expected_response = [sub1_response, sub2_response, sub3_response]
    response = historic_dabs_warning_summary_endpoint(filters)
    assert sort_results(response) == sort_results(expected_response)

    # Checking basic filters
    filters = {
        'quarters': [3],
        'fys': [2017],
        'agencies': ['089']
    }
    expected_response = [sub1_response]
    response = historic_dabs_warning_summary_endpoint(filters)
    assert sort_results(response) == sort_results(expected_response)

    filters = {
        'quarters': [1],
        'fys': [2019],
        'agencies': ['1125', '091']
    }
    expected_response = [sub2_response, sub3_response]
    response = historic_dabs_warning_summary_endpoint(filters)
    assert sort_results(response) == sort_results(expected_response)

    # Empty should return all
    filters = {
        'quarters': [],
        'fys': [],
        'agencies': []
    }
    expected_response = [sub1_response, sub2_response, sub3_response]
    response = historic_dabs_warning_summary_endpoint(filters)
    assert sort_results(response) == sort_results(expected_response)

    # Wrong agency format shouldn't return all
    filters = {
        'quarters': [1, 3],
        'fys': [2017, 2019],
        'agencies': ['09']
    }
    expected_error = "All codes in the agencies filter must be valid agency codes"
    with pytest.raises(ResponseException) as resp_except:
        historic_dabs_warning_summary_endpoint(filters)
    assert str(resp_except.value) == expected_error

    # Non-existent agency shouldn't return all
    filters = {
        'quarters': [1, 3],
        'fys': [2017, 2019],
        'agencies': ['090']
    }
    expected_error = "All codes in the agencies filter must be valid agency codes"
    with pytest.raises(ResponseException) as resp_except:
        historic_dabs_warning_summary_endpoint(filters)
    assert str(resp_except.value) == expected_error


@pytest.mark.usefixtures("job_constants")
@pytest.mark.usefixtures("user_constants")
def test_historic_dabs_warning_summary_agency_user(database, monkeypatch):
    sess = database.session

    user = setup_submissions(sess, admin=False)
    monkeypatch.setattr(dashboard_handler, 'g', Mock(user=user))

    # Responses
    sub1_response = {
        "submission_id": 1,
        "fy": 2017,
        "certifier": "Agency User",
        "quarter": 3,
        "agency": {
            "name": "CGAC",
            "code": "089"
        }
    }
    sub3_response = {
        "submission_id": 3,
        "fy": 2019,
        "certifier": "Agency User",
        "quarter": 1,
        "agency": {
            "name": "Other CGAC",
            "code": "091"
        }
    }

    # Perfect case - should still include sub3 cause the user still submitted it before switching agencies
    filters = {
        'quarters': [1, 3],
        'fys': [2017, 2019],
        'agencies': ['089', '1125', '091']
    }
    expected_response = [sub1_response, sub3_response]
    response = historic_dabs_warning_summary_endpoint(filters)
    assert sort_results(response) == sort_results(expected_response)

    # Admin - Checking basic filters
    filters = {
        'quarters': [3],
        'fys': [2017],
        'agencies': ['089']
    }
    expected_response = [sub1_response]
    response = historic_dabs_warning_summary_endpoint(filters)
    assert sort_results(response) == sort_results(expected_response)

    filters = {
        'quarters': [1],
        'fys': [2019],
        'agencies': ['091']
    }
    expected_response = [sub3_response]
    response = historic_dabs_warning_summary_endpoint(filters)
    assert sort_results(response) == sort_results(expected_response)

    # Empty should return all (that the user has access to)
    filters = {
        'quarters': [],
        'fys': [],
        'agencies': []
    }
    expected_response = [sub1_response, sub3_response]
    response = historic_dabs_warning_summary_endpoint(filters)
    assert sort_results(response) == sort_results(expected_response)

    # Special cases

    # Wrong agency format shouldn't return all
    filters = {
        'quarters': [1, 3],
        'fys': [2017, 2019],
        'agencies': ['09']
    }
    expected_error = "All codes in the agencies filter must be valid agency codes"
    with pytest.raises(ResponseException) as resp_except:
        historic_dabs_warning_summary_endpoint(filters)
    assert str(resp_except.value) == expected_error

    # Non-existent agency shouldn't return all
    filters = {
        'quarters': [1, 3],
        'fys': [2017, 2019],
        'agencies': ['090']
    }
    expected_error = "All codes in the agencies filter must be valid agency codes"
    with pytest.raises(ResponseException) as resp_except:
        historic_dabs_warning_summary_endpoint(filters)
    assert str(resp_except.value) == expected_error
