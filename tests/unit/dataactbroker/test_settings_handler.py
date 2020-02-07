import json
import pytest

from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from dataactcore.models.lookups import FILE_TYPE_DICT_LETTER_ID, RULE_SEVERITY_DICT, RULE_IMPACT_DICT
from dataactcore.models.validationModels import RuleSql, RuleSetting
from dataactbroker.handlers import settings_handler
from dataactbroker.handlers.dashboard_handler import generate_file_type
from dataactcore.scripts.initialize import load_default_rule_settings
from dataactcore.utils.responseException import ResponseException


def rule_settings_endpoint(agency_code, file):
    json_response = settings_handler.list_rule_settings(agency_code=agency_code, file=file)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def setup_tests(sess):
    db_objects = []

    # Setup agencies
    cgac1 = CGACFactory(cgac_code='097', agency_name='CGAC')
    frec = FRECFactory(cgac=cgac1, frec_code='1125', agency_name='FREC')
    db_objects.extend([cgac1, frec])

    # Setup rules
    rsql_a1 = RuleSql(rule_sql='', rule_label='A1', rule_error_message='A1 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['warning'],
                      rule_cross_file_flag=False)
    rsql_a2 = RuleSql(rule_sql='', rule_label='A2', rule_error_message='A2 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                      rule_cross_file_flag=False)
    rsql_a3 = RuleSql(rule_sql='', rule_label='A3', rule_error_message='A3 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['warning'],
                      rule_cross_file_flag=False)
    rsql_a4 = RuleSql(rule_sql='', rule_label='A4', rule_error_message='A4 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                      rule_cross_file_flag=False)
    rsql_b1 = RuleSql(rule_sql='', rule_label='B1', rule_error_message='B1 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['B'], rule_severity_id=RULE_SEVERITY_DICT['warning'],
                      rule_cross_file_flag=False)
    rsql_b2 = RuleSql(rule_sql='', rule_label='B2', rule_error_message='B2 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['B'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                      rule_cross_file_flag=False)
    rsql_b3 = RuleSql(rule_sql='', rule_label='B3', rule_error_message='B3 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['B'], rule_severity_id=RULE_SEVERITY_DICT['warning'],
                      rule_cross_file_flag=False)
    rsql_b4 = RuleSql(rule_sql='', rule_label='B4', rule_error_message='B4 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['B'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                      rule_cross_file_flag=False)
    sess.add_all([rsql_a1, rsql_a2, rsql_a3, rsql_a4, rsql_b1, rsql_b2, rsql_b3, rsql_b4])

    # Setup default rules
    load_default_rule_settings(sess)

    sess.add_all(db_objects)
    sess.commit()


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('validation_constants')
def test_list_rule_settings_input_errors(database):
    """ Testing list_rule_settings function when invalid parameters are passed in. """
    sess = database.session
    setup_tests(sess)

    def test_failure(agency_code, file):
        try:
            settings_handler.list_rule_settings(agency_code=agency_code, file=file)
        except ResponseException as e:
            return e

    results = test_failure(agency_code=None, file=None)
    assert results.status == 400
    assert str(results) == 'Invalid file type: None'

    results = test_failure(agency_code='', file='')
    assert results.status == 400
    assert str(results) == 'Invalid file type: '

    results = test_failure(agency_code='BAD', file='A')
    assert results.status == 400
    assert str(results) == 'Invalid agency_code: BAD'

    results = test_failure(agency_code='097', file='BAD')
    assert results.status == 400
    assert str(results) == 'Invalid file type: BAD'


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('validation_constants')
def test_list_rule_settings(database):
    """ Testing list_rule_settings function. """
    sess = database.session
    setup_tests(sess)

    # Setting up expectations
    rule_a1 = {
        'significance': 1,
        'label': 'A1',
        'description': 'A1 Description',
        'impact': 'high'
    }
    rule_a3 = {
        'significance': 2,
        'label': 'A3',
        'description': 'A3 Description',
        'impact': 'high'
    }
    rule_a2 = {
        'significance': 1,
        'label': 'A2',
        'description': 'A2 Description',
        'impact': 'high'
    }
    rule_a4 = {
        'significance': 2,
        'label': 'A4',
        'description': 'A4 Description',
        'impact': 'high'
    }
    rule_b1 = {
        'significance': 1,
        'label': 'B1',
        'description': 'B1 Description',
        'impact': 'high'
    }
    rule_b3 = {
        'significance': 2,
        'label': 'B3',
        'description': 'B3 Description',
        'impact': 'high'
    }
    rule_b2 = {
        'significance': 1,
        'label': 'B2',
        'description': 'B2 Description',
        'impact': 'high'
    }
    rule_b4 = {
        'significance': 2,
        'label': 'B4',
        'description': 'B4 Description',
        'impact': 'high'
    }
    # Normal results
    results = rule_settings_endpoint(agency_code='097', file='A')
    assert results['errors'] == [rule_a2, rule_a4]
    assert results['warnings'] == [rule_a1, rule_a3]

    results = rule_settings_endpoint(agency_code='1125', file='B')
    assert results['errors'] == [rule_b2, rule_b4]
    assert results['warnings'] == [rule_b1, rule_b3]

    # Testing with a populated agency with reverse order of significance
    cgac_rule_settings = []
    priorities = {}
    for rule in sess.query(RuleSql).order_by(RuleSql.rule_sql_id).all():
        file_type = generate_file_type(rule.file_id, rule.target_file_id)
        if file_type not in priorities:
            priorities[file_type] = {'error': 2, 'warning': 2}

        if rule.rule_severity_id == RULE_SEVERITY_DICT['warning']:
            cgac_rule_settings.append(RuleSetting(rule_id=rule.rule_sql_id, agency_code='1125',
                                                  priority=priorities[file_type]['warning'],
                                                  impact_id=RULE_IMPACT_DICT['low']))
            priorities[file_type]['warning'] -= 1
        else:
            cgac_rule_settings.append(RuleSetting(rule_id=rule.rule_sql_id, agency_code='1125',
                                                  priority=priorities[file_type]['error'],
                                                  impact_id=RULE_IMPACT_DICT['medium']))
            priorities[file_type]['error'] -= 1
    sess.add_all(cgac_rule_settings)

    rule_a1['significance'] = rule_b1['significance'] = rule_a2['significance'] = rule_b2['significance'] = 2
    rule_a1['impact'] = rule_b1['impact'] = rule_a3['impact'] = rule_b3['impact'] = 'low'
    rule_a3['significance'] = rule_b3['significance'] = rule_a4['significance'] = rule_b4['significance'] = 1
    rule_a2['impact'] = rule_b2['impact'] = rule_a4['impact'] = rule_b4['impact'] = 'medium'

    results = rule_settings_endpoint(agency_code='1125', file='A')
    assert results['errors'] == [rule_a4, rule_a2]
    assert results['warnings'] == [rule_a3, rule_a1]
    results = rule_settings_endpoint(agency_code='1125', file='B')
    assert results['errors'] == [rule_b4, rule_b2]
    assert results['warnings'] == [rule_b3, rule_b1]
