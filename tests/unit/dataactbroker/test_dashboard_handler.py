import json
import pytest
import copy
from datetime import datetime
from unittest.mock import Mock

from tests.unit.dataactcore.factories.user import UserFactory
from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory, JobFactory
from dataactcore.models.userModel import UserAffiliation
from dataactcore.models.lookups import (PERMISSION_TYPE_DICT, PUBLISH_STATUS_DICT, FILE_TYPE_DICT_LETTER_ID,
                                        RULE_SEVERITY_DICT)
from dataactcore.models.validationModels import RuleSql
from dataactcore.models.errorModels import CertifiedErrorMetadata
from dataactbroker.helpers.generic_helper import fy
from dataactbroker.helpers import filters_helper
from dataactbroker.handlers import dashboard_handler
from dataactcore.utils.responseException import ResponseException


def historic_dabs_warning_summary_endpoint(filters):
    json_response = dashboard_handler.historic_dabs_warning_summary(filters)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def historic_dabs_warning_graphs_endpoint(filters):
    json_response = dashboard_handler.historic_dabs_warning_graphs(filters)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def test_validate_historic_dashboard_filters():
    def assert_validation(filters, expected_response, graphs=False):
        with pytest.raises(ResponseException) as resp_except:
            dashboard_handler.validate_historic_dashboard_filters(filters, graphs=graphs)

        assert resp_except.value.status == 400
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

    # wrong files
    filters = {'quarters': [1, 3], 'fys': [2017, 2019], 'agencies': ['097'], 'files': ['R2D2', 'C3P0'], 'rules': []}
    assert_validation(filters, 'Files must be a list of one or more of the following, or an empty list: '
                      'A, B, C, cross-AB, cross-BC, cross-CD1, cross-CD2', graphs=True)
    filters = {'quarters': [1, 3], 'fys': [2017, 2019], 'agencies': ['097'], 'files': [2, 3], 'rules': []}
    assert_validation(filters, 'Files must be a list of one or more of the following, or an empty list: '
                      'A, B, C, cross-AB, cross-BC, cross-CD1, cross-CD2', graphs=True)

    # wrong rules
    filters = {'quarters': [1, 3], 'fys': [2017, 2019], 'agencies': ['097'], 'files': ['A', 'B'], 'rules': [2, 3]}
    assert_validation(filters, 'Rules must be a list of strings, or an empty list.', graphs=True)


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

    # Setup validation jobs
    sub1_a = JobFactory(submission=sub1, file_type_id=FILE_TYPE_DICT_LETTER_ID['A'])
    sub1_ab = JobFactory(submission=sub1, file_type_id=None)
    sub2_b = JobFactory(submission=sub2, file_type_id=FILE_TYPE_DICT_LETTER_ID['B'])
    sub3_c = JobFactory(submission=sub3, file_type_id=FILE_TYPE_DICT_LETTER_ID['C'])
    db_objects.extend([sub1_a, sub1_ab, sub2_b, sub3_c])

    # Setup certified error metadata
    sub1_a1 = CertifiedErrorMetadata(job=sub1_a, original_rule_label='A1', occurrences=20,
                                     file_type_id=FILE_TYPE_DICT_LETTER_ID['A'],
                                     target_file_type_id=None)
    sub1_a2 = CertifiedErrorMetadata(job=sub1_a, original_rule_label='A2', occurrences=30,
                                     file_type_id=FILE_TYPE_DICT_LETTER_ID['A'],
                                     target_file_type_id=None)
    sub1_ab1 = CertifiedErrorMetadata(job=sub1_ab, original_rule_label='A3', occurrences=70,
                                      file_type_id=FILE_TYPE_DICT_LETTER_ID['A'],
                                      target_file_type_id=FILE_TYPE_DICT_LETTER_ID['B'])
    sub1_ab2 = CertifiedErrorMetadata(job=sub1_ab, original_rule_label='B1', occurrences=130,
                                      file_type_id=FILE_TYPE_DICT_LETTER_ID['B'],
                                      target_file_type_id=FILE_TYPE_DICT_LETTER_ID['A'])
    sub2_b1 = CertifiedErrorMetadata(job=sub2_b, original_rule_label='B2', occurrences=70,
                                     file_type_id=FILE_TYPE_DICT_LETTER_ID['B'],
                                     target_file_type_id=None)
    sub2_bc1 = CertifiedErrorMetadata(job=sub2_b, original_rule_label='B3', occurrences=120,
                                      file_type_id=FILE_TYPE_DICT_LETTER_ID['B'],
                                      target_file_type_id=FILE_TYPE_DICT_LETTER_ID['C'])
    # no warnings for sub3
    db_objects.extend([sub1_a1, sub1_a2, sub1_ab1, sub1_ab2, sub2_b1, sub2_bc1])

    sess.add_all(db_objects)
    sess.commit()

    user = agency_user if not admin else admin_user
    return user


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('user_constants')
def test_historic_dabs_warning_summary_admin(database, monkeypatch):
    sess = database.session

    user = setup_submissions(sess, admin=True)
    monkeypatch.setattr(filters_helper, 'g', Mock(user=user))

    # Responses
    sub1_response = {
        'submission_id': 1,
        'fy': 2017,
        'certifier': 'Agency User',
        'quarter': 3,
        'agency': {
            'name': 'CGAC',
            'code': '089'
        }
    }
    sub2_response = {
        'submission_id': 2,
        'fy': 2019,
        'certifier': 'Administrator',
        'quarter': 1,
        'agency': {
            'name': 'FREC',
            'code': '1125'
        }
    }
    sub3_response = {
        'submission_id': 3,
        'fy': 2019,
        'certifier': 'Agency User',
        'quarter': 1,
        'agency': {
            'name': 'Other CGAC',
            'code': '091'
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
    expected_error = 'All codes in the agency_codes filter must be valid agency codes'
    with pytest.raises(ResponseException) as resp_except:
        historic_dabs_warning_summary_endpoint(filters)
    assert str(resp_except.value) == expected_error

    # Non-existent agency shouldn't return all
    filters = {
        'quarters': [1, 3],
        'fys': [2017, 2019],
        'agencies': ['090']
    }
    expected_error = 'All codes in the agency_codes filter must be valid agency codes'
    with pytest.raises(ResponseException) as resp_except:
        historic_dabs_warning_summary_endpoint(filters)
    assert str(resp_except.value) == expected_error


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('user_constants')
def test_historic_dabs_warning_summary_agency_user(database, monkeypatch):
    sess = database.session

    user = setup_submissions(sess, admin=False)
    monkeypatch.setattr(filters_helper, 'g', Mock(user=user))

    # Responses
    sub1_response = {
        'submission_id': 1,
        'fy': 2017,
        'certifier': 'Agency User',
        'quarter': 3,
        'agency': {
            'name': 'CGAC',
            'code': '089'
        }
    }
    sub3_response = {
        'submission_id': 3,
        'fy': 2019,
        'certifier': 'Agency User',
        'quarter': 1,
        'agency': {
            'name': 'Other CGAC',
            'code': '091'
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
    expected_error = 'All codes in the agency_codes filter must be valid agency codes'
    with pytest.raises(ResponseException) as resp_except:
        historic_dabs_warning_summary_endpoint(filters)
    assert str(resp_except.value) == expected_error

    # Non-existent agency shouldn't return all
    filters = {
        'quarters': [1, 3],
        'fys': [2017, 2019],
        'agencies': ['090']
    }
    expected_error = 'All codes in the agency_codes filter must be valid agency codes'
    with pytest.raises(ResponseException) as resp_except:
        historic_dabs_warning_summary_endpoint(filters)
    assert str(resp_except.value) == expected_error


def test_list_rule_labels_input_errors():
    """ Testing list_rule_labels function when invalid parameters are passed in. """

    # sending a list of files with FABS
    results = dashboard_handler.list_rule_labels(['A', 'B'], fabs=True)
    assert results.status_code == 400
    assert results.json['message'] == 'Files list must be empty for FABS rules'

    # Sending multiple file types that aren't valid
    results = dashboard_handler.list_rule_labels(['A', 'B', 'red', 'green'])
    assert results.status_code == 400
    assert results.json['message'] == 'The following are not valid file types: red, green'

    # Wrong case file
    results = dashboard_handler.list_rule_labels(['a'])
    assert results.status_code == 400
    assert results.json['message'] == 'The following are not valid file types: a'


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('validation_constants')
def test_list_rule_labels(database):
    """ Testing list_rule_labels function. """
    sess = database.session

    rule_sql_1 = RuleSql(rule_sql='', rule_label='FABS1', rule_error_message='', query_name='',
                         file_id=FILE_TYPE_DICT_LETTER_ID['FABS'], rule_severity_id=RULE_SEVERITY_DICT['warning'],
                         rule_cross_file_flag=False)
    rule_sql_2 = RuleSql(rule_sql='', rule_label='FABS2', rule_error_message='', query_name='',
                         file_id=FILE_TYPE_DICT_LETTER_ID['FABS'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                         rule_cross_file_flag=False)
    rule_sql_3 = RuleSql(rule_sql='', rule_label='A1', rule_error_message='', query_name='',
                         file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['warning'],
                         rule_cross_file_flag=False)
    rule_sql_4 = RuleSql(rule_sql='', rule_label='AB1', rule_error_message='', query_name='',
                         file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['warning'],
                         rule_cross_file_flag=True, target_file_id=FILE_TYPE_DICT_LETTER_ID['B'])
    rule_sql_5 = RuleSql(rule_sql='', rule_label='AB2', rule_error_message='', query_name='',
                         file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                         rule_cross_file_flag=True, target_file_id=FILE_TYPE_DICT_LETTER_ID['B'])
    sess.add_all([rule_sql_1, rule_sql_2, rule_sql_3, rule_sql_4, rule_sql_5])

    # Getting all FABS warning labels
    results = dashboard_handler.list_rule_labels([], fabs=True)
    assert results.json['labels'] == ['FABS1']

    # Getting all FABS error labels
    results = dashboard_handler.list_rule_labels([], 'error', True)
    assert results.json['labels'] == ['FABS2']

    # Getting all FABS labels
    results = dashboard_handler.list_rule_labels([], 'mixed', True)
    assert sorted(results.json['labels']) == ['FABS1', 'FABS2']

    # Getting all DABS labels
    results = dashboard_handler.list_rule_labels([], 'mixed')
    assert sorted(results.json['labels']) == ['A1', 'AB1', 'AB2']

    # Getting DABS warning labels for files A, B, and cross-AB (one has no labels, this is intentional)
    results = dashboard_handler.list_rule_labels(['A', 'B', 'cross-AB'])
    assert sorted(results.json['labels']) == ['A1', 'AB1']

    # Getting DABS warning labels for file A
    results = dashboard_handler.list_rule_labels(['A'])
    assert sorted(results.json['labels']) == ['A1']

    # Getting DABS error labels for cross-AB
    results = dashboard_handler.list_rule_labels(['cross-AB'], 'error')
    assert sorted(results.json['labels']) == ['AB2']


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('user_constants')
def test_historic_dabs_warning_graphs_admin(database, monkeypatch):
    sess = database.session

    user = setup_submissions(sess, admin=True)
    monkeypatch.setattr(filters_helper, 'g', Mock(user=user))

    # Shared Expected Data
    sub1_empty = {
        'submission_id': 1,
        'quarter': 3,
        'fy': 2017,
        'agency': {
            'name': 'CGAC',
            'code': '089'
        },
        'total_warnings': 0,
        'warnings': []
    }
    sub2_empty = {
        'submission_id': 2,
        'quarter': 1,
        'fy': 2019,
        'agency': {
            'name': 'FREC',
            'code': '1125'
        },
        'total_warnings': 0,
        'warnings': []
    }
    sub3_empty = {
        'submission_id': 3,
        'quarter': 1,
        'fy': 2019,
        'agency': {
            'name': 'Other CGAC',
            'code': '091'
        },
        'total_warnings': 0,
        'warnings': []
    }
    all_subs_empty_results = [sub1_empty, sub2_empty, sub3_empty]

    a1_warning = {'label': 'A1', 'instances': 20, 'percent_total': 40}
    a2_warning = {'label': 'A2', 'instances': 30, 'percent_total': 60}
    a_single = {'total_warnings': 50, 'warnings': [a1_warning, a2_warning]}
    sub1_single = copy.deepcopy(sub1_empty)
    sub1_single.update(a_single)

    a1_warning_filtered = {'label': 'A1', 'instances': 20, 'percent_total': 100}
    a_single_filtered = {'total_warnings': 20, 'warnings': [a1_warning_filtered]}
    a_single_filtered['warnings'][0]['percent_total'] = 100
    sub1_single_filtered = copy.deepcopy(sub1_empty)
    sub1_single_filtered.update(a_single_filtered)

    b2_warning = {'label': 'B2', 'instances': 70, 'percent_total': 100}
    b_populated = {'total_warnings': 70, 'warnings': [b2_warning]}
    sub2_single = copy.deepcopy(sub2_empty)
    sub2_single.update(b_populated)

    a3_warning = {'label': 'A3', 'instances': 70, 'percent_total': 35}
    b1_warning = {'label': 'B1', 'instances': 130, 'percent_total': 65}
    ab_cross = {'total_warnings': 200, 'warnings': [a3_warning, b1_warning]}
    sub1_cross = copy.deepcopy(sub1_empty)
    sub1_cross.update(ab_cross)

    b3_warning = {'label': 'B3', 'instances': 120, 'percent_total': 100}
    bc_cross = {'total_warnings': 120, 'warnings': [b3_warning]}
    sub2_cross = copy.deepcopy(sub2_empty)
    sub2_cross.update(bc_cross)

    # Perfect case
    filters = {
        'quarters': [1, 3],
        'fys': [2017, 2019],
        'agencies': ['089', '1125', '091'],
        'files': ['A', 'B', 'C', 'cross-AB', 'cross-BC'],
        'rules': ['A1', 'A2', 'A3', 'B1', 'B2', 'B3']
    }
    expected_response = {
        'A': [sub1_single, sub2_empty, sub3_empty],
        'B': [sub1_empty, sub2_single, sub3_empty],
        'C': all_subs_empty_results,
        'cross-AB': [sub1_cross, sub2_empty, sub3_empty],
        'cross-BC': [sub1_empty, sub2_cross, sub3_empty]
    }
    response = historic_dabs_warning_graphs_endpoint(filters)
    assert response == expected_response

    # drop everything and get (mostly) same response, including empty cross-files
    filters = {
        'quarters': [],
        'fys': [],
        'agencies': [],
        'files': [],
        'rules': []
    }
    expected_response = {
        'A': [sub1_single, sub2_empty, sub3_empty],
        'B': [sub1_empty, sub2_single, sub3_empty],
        'C': all_subs_empty_results,
        'cross-AB': [sub1_cross, sub2_empty, sub3_empty],
        'cross-BC': [sub1_empty, sub2_cross, sub3_empty],
        'cross-CD1': all_subs_empty_results,
        'cross-CD2': all_subs_empty_results
    }
    response = historic_dabs_warning_graphs_endpoint(filters)
    assert response == expected_response

    # use each of the basic filters - just submission 1
    filters = {
        'quarters': [3],
        'fys': [2017],
        'agencies': ['089'],
        'files': ['A', 'B', 'C', 'cross-AB', 'cross-BC'],
        'rules': ['A1', 'A2', 'A3', 'B1', 'B2', 'B3']
    }
    expected_response = {
        'A': [sub1_single],
        'B': [sub1_empty],
        'C': [sub1_empty],
        'cross-AB': [sub1_cross],
        'cross-BC': [sub1_empty]
    }
    response = historic_dabs_warning_graphs_endpoint(filters)
    assert response == expected_response

    # use each of the detailed filters
    filters = {
        'quarters': [1, 3],
        'fys': [2017, 2019],
        'agencies': ['089', '1125', '091'],
        'files': ['A', 'C', 'cross-BC'],
        'rules': ['A1', 'A3', 'B1', 'B2']
    }
    expected_response = {
        'A': [sub1_single_filtered, sub2_empty, sub3_empty],
        'C': all_subs_empty_results,
        'cross-BC': [sub1_empty, sub2_empty, sub3_empty]
    }
    response = historic_dabs_warning_graphs_endpoint(filters)
    assert response == expected_response

    # completely empty response
    filters = {
        'quarters': [2],
        'fys': [2018],
        'agencies': ['091'],
        'files': ['A', 'B', 'C', 'cross-AB', 'cross-BC'],
        'rules': ['A1', 'A2', 'A3', 'B1', 'B2', 'B3']
    }
    expected_response = {
        'A': [],
        'B': [],
        'C': [],
        'cross-AB': [],
        'cross-BC': []
    }
    response = historic_dabs_warning_graphs_endpoint(filters)
    assert response == expected_response


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('user_constants')
def test_historic_dabs_warning_graphs_agency_user(database, monkeypatch):
    sess = database.session

    user = setup_submissions(sess, admin=False)
    monkeypatch.setattr(filters_helper, 'g', Mock(user=user))

    # Shared Expected Data
    sub1_empty = {
        'submission_id': 1,
        'quarter': 3,
        'fy': 2017,
        'agency': {
            'name': 'CGAC',
            'code': '089'
        },
        'total_warnings': 0,
        'warnings': []
    }
    sub3_empty = {
        'submission_id': 3,
        'quarter': 1,
        'fy': 2019,
        'agency': {
            'name': 'Other CGAC',
            'code': '091'
        },
        'total_warnings': 0,
        'warnings': []
    }

    a1_warning = {'label': 'A1', 'instances': 20, 'percent_total': 40}
    a2_warning = {'label': 'A2', 'instances': 30, 'percent_total': 60}
    a_single = {'total_warnings': 50, 'warnings': [a1_warning, a2_warning]}
    sub1_single = copy.deepcopy(sub1_empty)
    sub1_single.update(a_single)

    a3_warning = {'label': 'A3', 'instances': 70, 'percent_total': 35}
    b1_warning = {'label': 'B1', 'instances': 130, 'percent_total': 65}
    ab_cross = {'total_warnings': 200, 'warnings': [a3_warning, b1_warning]}
    sub1_cross = copy.deepcopy(sub1_empty)
    sub1_cross.update(ab_cross)

    # Get everything, notice this is already just submission 1 (their current agency) and sub3 (they made it)
    filters = {
        'quarters': [],
        'fys': [],
        'agencies': [],
        'files': [],
        'rules': []
    }
    expected_response = {
        'A': [sub1_single, sub3_empty],
        'B': [sub1_empty, sub3_empty],
        'C': [sub1_empty, sub3_empty],
        'cross-AB': [sub1_cross, sub3_empty],
        'cross-BC': [sub1_empty, sub3_empty],
        'cross-CD1': [sub1_empty, sub3_empty],
        'cross-CD2': [sub1_empty, sub3_empty]
    }
    response = historic_dabs_warning_graphs_endpoint(filters)
    assert response == expected_response
