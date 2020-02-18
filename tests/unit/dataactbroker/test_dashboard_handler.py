import json
import pytest
import copy
from datetime import datetime, timedelta
from unittest.mock import Mock

from tests.unit.dataactcore.factories.user import UserFactory
from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory, JobFactory, QuarterlyRevalidationThresholdFactory
from dataactcore.models.userModel import UserAffiliation
from dataactcore.models.lookups import (PERMISSION_TYPE_DICT, PUBLISH_STATUS_DICT, FILE_TYPE_DICT_LETTER_ID,
                                        RULE_SEVERITY_DICT, RULE_IMPACT_DICT)
from dataactcore.models.jobModels import Submission
from dataactcore.models.validationModels import RuleSql, RuleSetting
from dataactcore.models.errorModels import CertifiedErrorMetadata, ErrorMetadata
from dataactbroker.helpers.generic_helper import fy
from dataactbroker.helpers import filters_helper
from dataactbroker.handlers import dashboard_handler
from dataactbroker.handlers import agency_handler
from dataactcore.utils.responseException import ResponseException


def historic_dabs_warning_summary_endpoint(filters):
    json_response = dashboard_handler.historic_dabs_warning_summary(filters)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def historic_dabs_warning_graphs_endpoint(filters):
    json_response = dashboard_handler.historic_dabs_warning_graphs(filters)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def historic_dabs_warning_table_endpoint(filters, page=1, limit=5, sort='period', order='desc'):
    json_response = dashboard_handler.historic_dabs_warning_table(filters, page, limit, sort, order)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def active_submission_overview_endpoint(submission, file, error_level):
    json_response = dashboard_handler.active_submission_overview(submission, file, error_level)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def get_impact_counts_endpoint(submission, file, error_level):
    json_response = dashboard_handler.get_impact_counts(submission, file, error_level)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def active_submission_table_endpoint(submission, file, error_level, page=1, limit=5, sort='significance', order='desc'):
    json_response = dashboard_handler.active_submission_table(submission, file, error_level, page, limit, sort, order)
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
    return sorted(results, key=lambda d: d['agency_name'])


def setup_submissions(sess, admin=False):
    db_objects = []

    # Setup agencies
    cgac1 = CGACFactory(cgac_code='089', agency_name='CGAC')
    cgac2 = CGACFactory(cgac_code='011', agency_name='CGAC Associated with FREC')
    cgac3 = CGACFactory(cgac_code='091', agency_name='Other CGAC')
    cgac4 = CGACFactory(cgac_code='229', agency_name='Unused CGAC')
    cgac5 = CGACFactory(cgac_code='230', agency_name='Unused CGAC 2')
    frec = FRECFactory(cgac=cgac2, frec_code='1125', agency_name='FREC')
    db_objects.extend([cgac1, cgac2, cgac3, cgac4, cgac5, frec])

    # Setup users and affiliations
    agency_user = UserFactory(name='Agency User', affiliations=[
        UserAffiliation(user_affiliation_id=1, cgac=cgac1, permission_type_id=PERMISSION_TYPE_DICT['reader']),
        UserAffiliation(user_affiliation_id=2, cgac=cgac4, permission_type_id=PERMISSION_TYPE_DICT['reader']),
        UserAffiliation(user_affiliation_id=3, cgac=cgac5, permission_type_id=PERMISSION_TYPE_DICT['reader'])
    ])
    admin_user = UserFactory(name='Administrator', website_admin=True)
    db_objects.extend([agency_user, admin_user])

    # Setup submissions
    sub1 = SubmissionFactory(submission_id=1, reporting_fiscal_period=9, reporting_fiscal_year=2017,
                             certifying_user_id=agency_user.user_id, cgac_code=cgac1.cgac_code, frec_code=None,
                             publish_status_id=PUBLISH_STATUS_DICT['updated'], d2_submission=False,
                             user_id=agency_user.user_id, is_quarter_format=True)
    sub2 = SubmissionFactory(submission_id=2, reporting_fiscal_period=3, reporting_fiscal_year=2019,
                             certifying_user_id=admin_user.user_id, cgac_code=None,
                             frec_code=frec.frec_code, publish_status_id=PUBLISH_STATUS_DICT['published'],
                             d2_submission=False, user_id=admin_user.user_id, is_quarter_format=True)
    sub3 = SubmissionFactory(submission_id=3, reporting_fiscal_period=3, reporting_fiscal_year=2019,
                             certifying_user_id=agency_user.user_id, cgac_code=cgac3.cgac_code,
                             frec_code=None, publish_status_id=PUBLISH_STATUS_DICT['published'],
                             d2_submission=False, user_id=agency_user.user_id, is_quarter_format=True)
    sub4 = SubmissionFactory(submission_id=4, reporting_fiscal_period=6, reporting_fiscal_year=2018,
                             certifying_user_id=agency_user.user_id, cgac_code=cgac3.cgac_code,
                             frec_code=None, publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                             d2_submission=False, user_id=agency_user.user_id, is_quarter_format=True)
    fabs_sub = SubmissionFactory(submission_id=5, reporting_fiscal_period=3, reporting_fiscal_year=2019,
                                 certifying_user_id=agency_user.user_id, cgac_code=cgac3.cgac_code,
                                 frec_code=None, publish_status_id=PUBLISH_STATUS_DICT['published'],
                                 d2_submission=True, user_id=agency_user.user_id, is_quarter_format=False)
    monthly_sub = SubmissionFactory(submission_id=6, reporting_fiscal_period=9, reporting_fiscal_year=2017,
                                    certifying_user_id=agency_user.user_id, cgac_code=cgac1.cgac_code, frec_code=None,
                                    publish_status_id=PUBLISH_STATUS_DICT['unpublished'], d2_submission=False,
                                    user_id=agency_user.user_id, is_quarter_format=False,
                                    reporting_start_date=datetime(2017, 6, 1))
    db_objects.extend([sub1, sub2, sub3, sub4, fabs_sub, monthly_sub])

    # Setup validation jobs
    sub1_a = JobFactory(submission=sub1, file_type_id=FILE_TYPE_DICT_LETTER_ID['A'],
                        original_filename='sub1_filea.csv')
    sub1_b = JobFactory(submission=sub1, file_type_id=FILE_TYPE_DICT_LETTER_ID['B'],
                        original_filename='sub1_fileb.csv')
    sub1_ab = JobFactory(submission=sub1, file_type_id=None)
    sub2_b = JobFactory(submission=sub2, file_type_id=FILE_TYPE_DICT_LETTER_ID['B'],
                        original_filename='sub2_fileb.csv')
    sub2_c = JobFactory(submission=sub2, file_type_id=FILE_TYPE_DICT_LETTER_ID['C'],
                        original_filename='sub2_filec.csv')
    sub2_bc = JobFactory(submission=sub2, file_type_id=None)
    sub3_c = JobFactory(submission=sub3, file_type_id=FILE_TYPE_DICT_LETTER_ID['C'],
                        original_filename='sub3_filec.csv')
    db_objects.extend([sub1_a, sub1_b, sub1_ab, sub2_b, sub2_c, sub2_bc, sub3_c])

    # Setup a couple of rules
    rule_a1 = RuleSql(rule_sql='', rule_label='A1', rule_error_message='first rule', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['warning'],
                      rule_cross_file_flag=False, category='completeness')
    rule_a2 = RuleSql(rule_sql='', rule_label='A2', rule_error_message='second rule', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['warning'],
                      rule_cross_file_flag=False, category='accuracy')
    rule_ab1 = RuleSql(rule_sql='', rule_label='A3', rule_error_message='first cross rule', query_name='',
                       file_id=FILE_TYPE_DICT_LETTER_ID['A'], target_file_id=FILE_TYPE_DICT_LETTER_ID['B'],
                       rule_severity_id=RULE_SEVERITY_DICT['warning'], rule_cross_file_flag=True, category='existence')
    rule_ab2 = RuleSql(rule_sql='', rule_label='B1', rule_error_message='second cross rule', query_name='',
                       file_id=FILE_TYPE_DICT_LETTER_ID['B'], target_file_id=FILE_TYPE_DICT_LETTER_ID['A'],
                       rule_severity_id=RULE_SEVERITY_DICT['warning'], rule_cross_file_flag=True, category='existence')
    db_objects.extend([rule_a1, rule_a2, rule_ab1, rule_ab2])

    # Setup error metadata
    sub1_a1 = ErrorMetadata(job=sub1_a, original_rule_label=rule_a1.rule_label, occurrences=20,
                            file_type_id=FILE_TYPE_DICT_LETTER_ID['A'], target_file_type_id=None,
                            rule_failed=rule_a1.rule_error_message, severity_id=rule_a1.rule_severity_id)
    sub1_a2 = ErrorMetadata(job=sub1_a, original_rule_label=rule_a2.rule_label, occurrences=30,
                            file_type_id=FILE_TYPE_DICT_LETTER_ID['A'], target_file_type_id=None,
                            rule_failed=rule_a2.rule_error_message, severity_id=rule_a2.rule_severity_id)
    sub1_ab1 = ErrorMetadata(job=sub1_ab, original_rule_label=rule_ab1.rule_label, occurrences=70,
                             file_type_id=rule_ab1.file_id, target_file_type_id=rule_ab1.target_file_id,
                             rule_failed=rule_ab1.rule_error_message, severity_id=rule_ab1.rule_severity_id)
    sub1_ab2 = ErrorMetadata(job=sub1_ab, original_rule_label=rule_ab2.rule_label, occurrences=130,
                             file_type_id=rule_ab2.file_id, target_file_type_id=rule_ab2.target_file_id,
                             rule_failed=rule_ab2.rule_error_message, severity_id=rule_ab2.rule_severity_id)
    sub2_b1 = ErrorMetadata(job=sub2_b, original_rule_label='B2', occurrences=70,
                            file_type_id=FILE_TYPE_DICT_LETTER_ID['B'], target_file_type_id=None,
                            rule_failed='first B rule', severity_id=RULE_SEVERITY_DICT['warning'])
    sub2_bc1 = ErrorMetadata(job=sub2_bc, original_rule_label='B3', occurrences=120,
                             file_type_id=FILE_TYPE_DICT_LETTER_ID['B'],
                             target_file_type_id=FILE_TYPE_DICT_LETTER_ID['C'], rule_failed='another cross rule',
                             severity_id=RULE_SEVERITY_DICT['warning'])
    sub3_c1 = ErrorMetadata(job=sub3_c, original_rule_label='C1', occurrences=20,
                            file_type_id=FILE_TYPE_DICT_LETTER_ID['C'], target_file_type_id=None,
                            rule_failed='first rule', severity_id=RULE_SEVERITY_DICT['fatal'])
    sub3_c2 = ErrorMetadata(job=sub3_c, original_rule_label='C2', occurrences=15,
                            file_type_id=FILE_TYPE_DICT_LETTER_ID['C'], target_file_type_id=None,
                            rule_failed='first rule', severity_id=RULE_SEVERITY_DICT['warning'])

    db_objects.extend([sub1_a1, sub1_a2, sub1_ab1, sub1_ab2, sub2_b1, sub2_bc1, sub3_c1, sub3_c2])

    # Setup certified error metadata
    cert_sub1_a1 = CertifiedErrorMetadata(job=sub1_a, original_rule_label='A1', occurrences=20,
                                          file_type_id=FILE_TYPE_DICT_LETTER_ID['A'],
                                          target_file_type_id=None, rule_failed='first rule')
    cert_sub1_a2 = CertifiedErrorMetadata(job=sub1_a, original_rule_label='A2', occurrences=30,
                                          file_type_id=FILE_TYPE_DICT_LETTER_ID['A'],
                                          target_file_type_id=None, rule_failed='second rule')
    cert_sub1_ab1 = CertifiedErrorMetadata(job=sub1_ab, original_rule_label='A3', occurrences=70,
                                           file_type_id=FILE_TYPE_DICT_LETTER_ID['A'],
                                           target_file_type_id=FILE_TYPE_DICT_LETTER_ID['B'],
                                           rule_failed='first cross rule')
    cert_sub1_ab2 = CertifiedErrorMetadata(job=sub1_ab, original_rule_label='B1', occurrences=130,
                                           file_type_id=FILE_TYPE_DICT_LETTER_ID['B'],
                                           target_file_type_id=FILE_TYPE_DICT_LETTER_ID['A'],
                                           rule_failed='second cross rule')
    cert_sub2_b1 = CertifiedErrorMetadata(job=sub2_b, original_rule_label='B2', occurrences=70,
                                          file_type_id=FILE_TYPE_DICT_LETTER_ID['B'],
                                          target_file_type_id=None, rule_failed='first B rule')
    cert_sub2_bc1 = CertifiedErrorMetadata(job=sub2_bc, original_rule_label='B3', occurrences=120,
                                           file_type_id=FILE_TYPE_DICT_LETTER_ID['B'],
                                           target_file_type_id=FILE_TYPE_DICT_LETTER_ID['C'],
                                           rule_failed='another cross rule')
    # no warnings for sub3
    db_objects.extend([cert_sub1_a1, cert_sub1_a2, cert_sub1_ab1, cert_sub1_ab2, cert_sub2_b1, cert_sub2_bc1])

    # Setup quarterly revalidation threshold
    today = datetime.now().date()
    quart_3_year_2017 = QuarterlyRevalidationThresholdFactory(year=2017, quarter=3,
                                                              window_end=today - timedelta(days=1))
    quart_2_year_2018 = QuarterlyRevalidationThresholdFactory(year=2018, quarter=2,
                                                              window_end=today + timedelta(days=1))
    quart_1_year_2019 = QuarterlyRevalidationThresholdFactory(year=2019, quarter=1, window_end=today)
    db_objects.extend([quart_3_year_2017, quart_2_year_2018, quart_1_year_2019])

    sess.add_all(db_objects)
    sess.commit()

    # Create initial rule settings
    setting_a1 = RuleSetting(agency_code=None, rule_id=rule_a1.rule_sql_id, priority=1,
                             impact_id=RULE_IMPACT_DICT['high'])
    setting_a2 = RuleSetting(agency_code=None, rule_id=rule_a2.rule_sql_id, priority=2,
                             impact_id=RULE_IMPACT_DICT['high'])
    setting_ab1 = RuleSetting(agency_code=None, rule_id=rule_ab1.rule_sql_id, priority=1,
                              impact_id=RULE_IMPACT_DICT['high'])
    setting_ab2 = RuleSetting(agency_code=None, rule_id=rule_ab2.rule_sql_id, priority=2,
                              impact_id=RULE_IMPACT_DICT['high'])
    setting_ab1_cgac = RuleSetting(agency_code=sub1.cgac_code, rule_id=rule_ab1.rule_sql_id, priority=2,
                                   impact_id=RULE_IMPACT_DICT['low'])
    setting_ab2_cgac = RuleSetting(agency_code=sub1.cgac_code, rule_id=rule_ab2.rule_sql_id, priority=1,
                                   impact_id=RULE_IMPACT_DICT['high'])
    sess.add_all([setting_a1, setting_a2, setting_ab1, setting_ab2, setting_ab1_cgac, setting_ab2_cgac])
    sess.commit()

    user = agency_user if not admin else admin_user
    return user


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('user_constants')
@pytest.mark.usefixtures('validation_constants')
def test_historic_dabs_warning_summary_admin(database, monkeypatch):
    sess = database.session

    user = setup_submissions(sess, admin=True)
    monkeypatch.setattr(filters_helper, 'g', Mock(user=user))
    monkeypatch.setattr(agency_handler, 'g', Mock(user=user))

    # Responses
    sub1_response = {
        'agency_name': 'CGAC',
        'submissions': [
            {
                'submission_id': 1,
                'fy': 2017,
                'certifier': 'Agency User',
                'quarter': 3
            }
        ]
    }
    sub2_response = {
        'agency_name': 'FREC',
        'submissions': [
            {
                'submission_id': 2,
                'fy': 2019,
                'certifier': 'Administrator',
                'quarter': 1
            }
        ]
    }
    sub3_response = {
        'agency_name': 'Other CGAC',
        'submissions': [
            {
                'submission_id': 3,
                'fy': 2019,
                'certifier': 'Agency User',
                'quarter': 1
            }
        ]
    }
    unused_response = {
        'agency_name': 'Unused CGAC',
        'submissions': []
    }
    unused1_response = {
        'agency_name': 'Unused CGAC 2',
        'submissions': []
    }
    unused2_response = {
        'agency_name': 'CGAC Associated with FREC',
        'submissions': []
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
    expected_response = [sub1_response, sub2_response, sub3_response, unused_response, unused1_response,
                         unused2_response]
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
@pytest.mark.usefixtures('validation_constants')
def test_historic_dabs_warning_summary_agency_user(database, monkeypatch):
    sess = database.session

    user = setup_submissions(sess, admin=False)
    monkeypatch.setattr(filters_helper, 'g', Mock(user=user))
    monkeypatch.setattr(agency_handler, 'g', Mock(user=user))

    # Responses
    sub1_response = {
        'agency_name': 'CGAC',
        'submissions': [
            {
                'submission_id': 1,
                'fy': 2017,
                'certifier': 'Agency User',
                'quarter': 3
            }
        ]
    }
    sub3_response = {
        'agency_name': 'Other CGAC',
        'submissions': [
            {
                'submission_id': 3,
                'fy': 2019,
                'certifier': 'Agency User',
                'quarter': 1
            }
        ]
    }
    unused_response = {
        'agency_name': 'Unused CGAC',
        'submissions': []
    }
    unused1_response = {
        'agency_name': 'Unused CGAC 2',
        'submissions': []
    }

    # Perfect case - should still include sub3 cause the user still submitted it before switching agencies
    filters = {
        'quarters': [1, 3],
        'fys': [2017, 2019],
        'agencies': ['089', '1125', '091', '229']
    }
    expected_response = [sub1_response, sub3_response, unused_response]
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
    expected_response = [sub1_response, sub3_response, unused_response, unused1_response]
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
@pytest.mark.usefixtures('validation_constants')
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
@pytest.mark.usefixtures('validation_constants')
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


def test_validate_table_properties():
    def assert_validation(page, limit, order, sort, sort_options, expected_response):
        with pytest.raises(ResponseException) as resp_except:
            dashboard_handler.validate_table_properties(page, limit, order, sort, sort_options)

        assert resp_except.value.status == 400
        assert str(resp_except.value) == expected_response

    # Invalid pages
    assert_validation('this is a string', 1, 'desc', 'period', ['period'], 'Page must be an integer greater than 0')
    assert_validation(-5, 1, 'desc', 'period', ['period'], 'Page must be an integer greater than 0')
    assert_validation(0, 1, 'desc', 'period', ['period'], 'Page must be an integer greater than 0')

    # Invalid limit
    assert_validation(1, 'string test', 'desc', 'period', ['period'], 'Limit must be an integer greater than 0')
    assert_validation(1, -5, 'desc', 'period', ['period'], 'Limit must be an integer greater than 0')
    assert_validation(1, 0, 'desc', 'period', ['period'], 'Limit must be an integer greater than 0')

    # Order check
    assert_validation(1, 1, 'false', 'period', ['period'], 'Order must be "asc" or "desc"')
    assert_validation(1, 1, 55, 'period', ['period'], 'Order must be "asc" or "desc"')

    # Bad sort
    assert_validation(1, 1, 'desc', 'periods', ['period', 'range'], 'Sort must be one of: period, range')
    assert_validation(1, 1, 'desc', 'period', [], 'Sort must be one of: ')

    # Test success (we expect nothing to happen here, just run the function)
    dashboard_handler.validate_table_properties(1, 1, 'asc', 'period', ['period', 'range'])


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('user_constants')
@pytest.mark.usefixtures('validation_constants')
def test_historic_dabs_warning_table_admin(database, monkeypatch):
    sess = database.session

    user = setup_submissions(sess, admin=True)
    monkeypatch.setattr(filters_helper, 'g', Mock(user=user))

    # Shared Expected Data
    warning_sub1_a1 = {
        'submission_id': 1,
        'fy': 2017,
        'quarter': 3,
        'rule_label': 'A1',
        'instance_count': 20,
        'rule_description': 'first rule',
        'files': [
            {
                'type': 'A',
                'filename': 'sub1_filea.csv'
            }
        ]
    }
    warning_sub1_a2 = {
        'submission_id': 1,
        'fy': 2017,
        'quarter': 3,
        'rule_label': 'A2',
        'instance_count': 30,
        'rule_description': 'second rule',
        'files': [
            {
                'type': 'A',
                'filename': 'sub1_filea.csv'
            }
        ]
    }
    warning_sub1_ab1 = {
        'submission_id': 1,
        'fy': 2017,
        'quarter': 3,
        'rule_label': 'A3',
        'instance_count': 70,
        'rule_description': 'first cross rule',
        'files': [
            {
                'type': 'A',
                'filename': 'sub1_filea.csv'
            },
            {
                'type': 'B',
                'filename': 'sub1_fileb.csv'
            }
        ]
    }
    warning_sub1_ab2 = {
        'submission_id': 1,
        'fy': 2017,
        'quarter': 3,
        'rule_label': 'B1',
        'instance_count': 130,
        'rule_description': 'second cross rule',
        'files': [
            {
                'type': 'B',
                'filename': 'sub1_fileb.csv'
            },
            {
                'type': 'A',
                'filename': 'sub1_filea.csv'
            }
        ]
    }
    warning_sub2_b1 = {
        'submission_id': 2,
        'fy': 2019,
        'quarter': 1,
        'rule_label': 'B2',
        'instance_count': 70,
        'rule_description': 'first B rule',
        'files': [
            {
                'type': 'B',
                'filename': 'sub2_fileb.csv'
            }
        ]
    }
    warning_sub2_bc1 = {
        'submission_id': 2,
        'fy': 2019,
        'quarter': 1,
        'rule_label': 'B3',
        'instance_count': 120,
        'rule_description': 'another cross rule',
        'files': [
            {
                'type': 'B',
                'filename': 'sub2_fileb.csv'
            },
            {
                'type': 'C',
                'filename': 'sub2_filec.csv'
            }
        ]
    }

    # Perfect case, default values
    filters = {
        'quarters': [1, 3],
        'fys': [2017, 2019],
        'agencies': ['089', '1125', '091'],
        'files': ['A', 'B', 'C', 'cross-AB', 'cross-BC'],
        'rules': ['A1', 'A2', 'A3', 'B1', 'B2', 'B3']
    }
    expected_response = {
        'results': [warning_sub2_bc1, warning_sub2_b1, warning_sub1_ab2, warning_sub1_ab1, warning_sub1_a2],
        'page_metadata': {
            'total': 6,
            'page': 1,
            'limit': 5
        }
    }
    response = historic_dabs_warning_table_endpoint(filters)
    assert response == expected_response

    # Same filters as before only we want page 2
    expected_response = {
        'results': [warning_sub1_a1],
        'page_metadata': {
            'total': 6,
            'page': 2,
            'limit': 5
        }
    }
    response = historic_dabs_warning_table_endpoint(filters, page=2)
    assert response == expected_response

    # No filters, should get everything
    filters = {
        'quarters': [],
        'fys': [],
        'agencies': [],
        'files': [],
        'rules': []
    }
    expected_response = {
        'results': [warning_sub2_bc1, warning_sub2_b1, warning_sub1_ab2, warning_sub1_ab1, warning_sub1_a2],
        'page_metadata': {
            'total': 6,
            'page': 1,
            'limit': 5
        }
    }
    response = historic_dabs_warning_table_endpoint(filters)
    assert response == expected_response

    # Only some basic filters included, shouldn't include cross results
    filters = {
        'quarters': [1],
        'fys': [2019],
        'agencies': ['1125'],
        'files': ['B'],
        'rules': ['B2']
    }
    expected_response = {
        'results': [warning_sub2_b1],
        'page_metadata': {
            'total': 1,
            'page': 1,
            'limit': 5
        }
    }
    response = historic_dabs_warning_table_endpoint(filters)
    assert response == expected_response

    # Filtering with a set of filters that returns 0 results
    filters = {
        'quarters': [3],
        'fys': [2019],
        'agencies': [],
        'files': [],
        'rules': []
    }
    expected_response = {
        'results': [],
        'page_metadata': {
            'total': 0,
            'page': 1,
            'limit': 5
        }
    }
    response = historic_dabs_warning_table_endpoint(filters)
    assert response == expected_response

    # Return more results per page
    filters = {
        'quarters': [],
        'fys': [],
        'agencies': [],
        'files': [],
        'rules': []
    }
    expected_response = {
        'results': [warning_sub2_bc1, warning_sub2_b1, warning_sub1_ab2, warning_sub1_ab1, warning_sub1_a2,
                    warning_sub1_a1],
        'page_metadata': {
            'total': 6,
            'page': 1,
            'limit': 10
        }
    }
    response = historic_dabs_warning_table_endpoint(filters, limit=10)
    assert response == expected_response

    # change sort order
    expected_response = {
        'results': [warning_sub1_a1, warning_sub1_a2, warning_sub1_ab1, warning_sub1_ab2, warning_sub2_b1],
        'page_metadata': {
            'total': 6,
            'page': 1,
            'limit': 5
        }
    }
    response = historic_dabs_warning_table_endpoint(filters, order='asc')
    assert response == expected_response

    # change sort by occurrences
    expected_response = {
        'results': [warning_sub1_ab2, warning_sub2_bc1, warning_sub1_ab1, warning_sub2_b1, warning_sub1_a2],
        'page_metadata': {
            'total': 6,
            'page': 1,
            'limit': 5
        }
    }
    response = historic_dabs_warning_table_endpoint(filters, sort='instances')
    assert response == expected_response


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('user_constants')
@pytest.mark.usefixtures('validation_constants')
def test_active_submission_overview(database, monkeypatch):
    sess = database.session
    today = datetime.now().date()

    user = setup_submissions(sess, admin=True)
    monkeypatch.setattr(filters_helper, 'g', Mock(user=user))

    # FABS submissions should throw an error
    fabs_sub = sess.query(Submission).filter(Submission.d2_submission.is_(True)).first()
    expected_error = 'Submission must be a DABS submission.'
    with pytest.raises(ResponseException) as resp_except:
        dashboard_handler.active_submission_overview(fabs_sub, 'B', 'warning')
    assert str(resp_except.value) == expected_error

    # Monthly submission
    monthly_sub = sess.query(Submission).filter(Submission.d2_submission.is_(False),
                                                Submission.is_quarter_format.is_(False)).first()
    expected_response = {
        'submission_id': monthly_sub.submission_id,
        'icon_name': None,
        'agency_name': 'CGAC',
        'certification_deadline': 'N/A',
        'days_remaining': 'N/A',
        'reporting_period': '06 / 2017',
        'duration': 'Monthly',
        'file': 'File B',
        'number_of_rules': 0,
        'total_instances': 0
    }
    response = active_submission_overview_endpoint(monthly_sub, 'B', 'mixed')
    assert response == expected_response

    # Past due submission with some warnings
    past_due = sess.query(Submission).filter(Submission.reporting_fiscal_period == 9,
                                             Submission.reporting_fiscal_year == 2017,
                                             Submission.is_quarter_format.is_(True)).first()
    expected_response = {
        'submission_id': past_due.submission_id,
        'icon_name': None,
        'agency_name': 'CGAC',
        'certification_deadline': 'Past Due',
        'days_remaining': 'N/A',
        'reporting_period': 'FY 17 / Q3',
        'duration': 'Quarterly',
        'file': 'File A',
        'number_of_rules': 2,
        'total_instances': 50
    }
    response = active_submission_overview_endpoint(past_due, 'A', 'warning')
    assert response == expected_response

    # Due tomorrow with no warnings
    due_soon = sess.query(Submission).filter(Submission.reporting_fiscal_period == 6,
                                             Submission.reporting_fiscal_year == 2018).first()
    expected_response = {
        'submission_id': due_soon.submission_id,
        'icon_name': None,
        'agency_name': 'Other CGAC',
        'certification_deadline': (today + timedelta(days=1)).strftime('%B %-d, %Y'),
        'days_remaining': 1,
        'reporting_period': 'FY 18 / Q2',
        'duration': 'Quarterly',
        'file': 'File C',
        'number_of_rules': 0,
        'total_instances': 0
    }
    response = active_submission_overview_endpoint(due_soon, 'C', 'warning')
    assert response == expected_response

    # Due today with mixed errors and warnings
    due_today = sess.query(Submission).filter(Submission.submission_id == 3).first()
    expected_response = {
        'submission_id': due_today.submission_id,
        'icon_name': None,
        'agency_name': 'Other CGAC',
        'certification_deadline': today.strftime('%B %-d, %Y'),
        'days_remaining': 'Due Today',
        'reporting_period': 'FY 19 / Q1',
        'duration': 'Quarterly',
        'file': 'File C',
        'number_of_rules': 2,
        'total_instances': 35
    }
    response = active_submission_overview_endpoint(due_today, 'C', 'mixed')
    assert response == expected_response

    # Due today checking only errors
    expected_response['number_of_rules'] = 1
    expected_response['total_instances'] = 20
    response = active_submission_overview_endpoint(due_today, 'C', 'error')
    assert response == expected_response


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('user_constants')
@pytest.mark.usefixtures('validation_constants')
def test_get_impact_counts(database, monkeypatch):
    sess = database.session

    user = setup_submissions(sess, admin=True)
    monkeypatch.setattr(filters_helper, 'g', Mock(user=user))

    # FABS submissions should throw an error
    fabs_sub = sess.query(Submission).filter(Submission.d2_submission.is_(True)).first()
    expected_error = 'Submission must be a DABS submission.'
    with pytest.raises(ResponseException) as resp_except:
        dashboard_handler.get_impact_counts(fabs_sub, 'B', 'warning')
    assert str(resp_except.value) == expected_error

    # No occurrences of rules that have settings
    monthly_sub = sess.query(Submission).filter(Submission.d2_submission.is_(False),
                                                Submission.is_quarter_format.is_(False)).first()
    expected_response = {
        'low': {
            'total': 0,
            'rules': []
        },
        'medium': {
            'total': 0,
            'rules': []
        },
        'high': {
            'total': 0,
            'rules': []
        }
    }
    response = get_impact_counts_endpoint(monthly_sub, 'cross-AB', 'mixed')
    assert response == expected_response

    # Rule with occurrences
    sub1 = sess.query(Submission).filter(Submission.submission_id == 1).first()
    expected_response = {
        'low': {
            'total': 0,
            'rules': []
        },
        'medium': {
            'total': 0,
            'rules': []
        },
        'high': {
            'total': 2,
            'rules': [
                {
                    'rule_label': 'A1',
                    'instances': 20,
                    'rule_description': 'first rule'
                },
                {
                    'rule_label': 'A2',
                    'instances': 30,
                    'rule_description': 'second rule'
                }
            ]
        }
    }
    response = get_impact_counts_endpoint(sub1, 'A', 'mixed')
    assert response == expected_response

    # Rule with occurrences at different impacts (based on per-agency setting)
    expected_response = {
        'low': {
            'total': 1,
            'rules': [
                {
                    'rule_label': 'A3',
                    'instances': 70,
                    'rule_description': 'first cross rule'
                }
            ]
        },
        'medium': {
            'total': 0,
            'rules': []
        },
        'high': {
            'total': 1,
            'rules': [
                {
                    'rule_label': 'B1',
                    'instances': 130,
                    'rule_description': 'second cross rule'
                }
            ]
        }
    }
    response = get_impact_counts_endpoint(sub1, 'cross-AB', 'mixed')
    assert response == expected_response


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('user_constants')
@pytest.mark.usefixtures('validation_constants')
def test_active_submission_table(database, monkeypatch):
    sess = database.session

    user = setup_submissions(sess, admin=True)
    monkeypatch.setattr(filters_helper, 'g', Mock(user=user))

    # Shared results
    a1_error = {
        'significance': 1,
        'rule_label': 'A1',
        'instance_count': 20,
        'category': 'completeness',
        'impact': 'high',
        'rule_description': 'first rule'
    }
    a2_error = {
        'significance': 2,
        'rule_label': 'A2',
        'instance_count': 30,
        'category': 'accuracy',
        'impact': 'high',
        'rule_description': 'second rule'
    }

    # FABS submissions should throw an error
    fabs_sub = sess.query(Submission).filter(Submission.d2_submission.is_(True)).first()
    expected_error = 'Submission must be a DABS submission.'
    with pytest.raises(ResponseException) as resp_except:
        dashboard_handler.active_submission_table(fabs_sub, 'B', 'warning')
    assert str(resp_except.value) == expected_error

    # all defaults, no results
    monthly_sub = sess.query(Submission).filter(Submission.d2_submission.is_(False),
                                                Submission.is_quarter_format.is_(False)).first()
    expected_response = {
        'page_metadata': {
            'total': 0,
            'page': 1,
            'limit': 5,
            'submission_id': monthly_sub.submission_id,
            'files': ['A', 'B']
        },
        'results': []
    }
    response = active_submission_table_endpoint(monthly_sub, 'cross-AB', 'warning')
    assert response == expected_response

    # all defaults, 2 results
    sub1 = sess.query(Submission).filter(Submission.submission_id == 1).first()
    expected_response = {
        'page_metadata': {
            'total': 2,
            'page': 1,
            'limit': 5,
            'submission_id': sub1.submission_id,
            'files': ['A']
        },
        'results': [a2_error, a1_error]
    }
    response = active_submission_table_endpoint(sub1, 'A', 'warning')
    assert response == expected_response

    # page limit of 1
    expected_response = {
        'page_metadata': {
            'total': 2,
            'page': 1,
            'limit': 1,
            'submission_id': sub1.submission_id,
            'files': ['A']
        },
        'results': [a2_error]
    }
    response = active_submission_table_endpoint(sub1, 'A', 'warning', limit=1)
    assert response == expected_response

    # page limit of 1, page 2
    expected_response = {
        'page_metadata': {
            'total': 2,
            'page': 2,
            'limit': 1,
            'submission_id': sub1.submission_id,
            'files': ['A']
        },
        'results': [a1_error]
    }
    response = active_submission_table_endpoint(sub1, 'A', 'warning', page=2, limit=1)
    assert response == expected_response

    # order by impact
    expected_response = {
        'page_metadata': {
            'total': 2,
            'page': 1,
            'limit': 5,
            'submission_id': sub1.submission_id,
            'files': ['A']
        },
        'results': [a2_error, a1_error]
    }
    response = active_submission_table_endpoint(sub1, 'A', 'warning', sort='impact')
    assert response == expected_response

    # ascending order
    expected_response = {
        'page_metadata': {
            'total': 2,
            'page': 1,
            'limit': 5,
            'submission_id': sub1.submission_id,
            'files': ['A']
        },
        'results': [a1_error, a2_error]
    }
    response = active_submission_table_endpoint(sub1, 'A', 'warning', order='asc')
    assert response == expected_response

    # sub1 with specified priorities
    expected_response = {
        'page_metadata': {
            'total': 2,
            'page': 1,
            'limit': 5,
            'submission_id': sub1.submission_id,
            'files': ['A', 'B']
        },
        'results': [
            {
                'significance': 2,
                'rule_label': 'A3',
                'instance_count': 70,
                'category': 'existence',
                'impact': 'low',
                'rule_description': 'first cross rule'
            },
            {
                'significance': 1,
                'rule_label': 'B1',
                'instance_count': 130,
                'category': 'existence',
                'impact': 'high',
                'rule_description': 'second cross rule'
            }
        ]
    }
    response = active_submission_table_endpoint(sub1, 'cross-AB', 'warning')
    assert response == expected_response
