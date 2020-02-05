import json
import pytest

from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from dataactcore.models.lookups import FILE_TYPE_DICT_LETTER_ID, RULE_SEVERITY_DICT, RULE_IMPACT_DICT
from dataactcore.models.validationModels import RuleSql, RuleSetting
from dataactbroker.handlers import settings_handler
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
    rule_sql_1 = RuleSql(rule_sql='', rule_label='A1', rule_error_message='A1 Description', query_name='',
                         file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['warning'],
                         rule_cross_file_flag=False)
    rule_sql_2 = RuleSql(rule_sql='', rule_label='A2', rule_error_message='A2 Description', query_name='',
                         file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                         rule_cross_file_flag=False)
    rule_sql_3 = RuleSql(rule_sql='', rule_label='B1', rule_error_message='B1 Description', query_name='',
                         file_id=FILE_TYPE_DICT_LETTER_ID['B'], rule_severity_id=RULE_SEVERITY_DICT['warning'],
                         rule_cross_file_flag=False)
    rule_sql_4 = RuleSql(rule_sql='', rule_label='B2', rule_error_message='B2 Description', query_name='',
                         file_id=FILE_TYPE_DICT_LETTER_ID['B'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                         rule_cross_file_flag=False)
    sess.add_all([rule_sql_1, rule_sql_2, rule_sql_3, rule_sql_4])

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
    rule_a2 = {
        'significance': 2,
        'label': 'A2',
        'description': 'A2 Description',
        'impact': 'high'
    }
    rule_b1 = {
        'significance': 1,
        'label': 'B1',
        'description': 'B1 Description',
        'impact': 'high'
    }
    rule_b2 = {
        'significance': 2,
        'label': 'B2',
        'description': 'B2 Description',
        'impact': 'high'
    }
    # Normal results
    results = rule_settings_endpoint(agency_code='097', file='A')
    assert results['rules'] == [rule_a1, rule_a2]

    results = rule_settings_endpoint(agency_code='1125', file='B')
    assert results['rules'] == [rule_b1, rule_b2]

    # Change up the significances/impacts to show the change in order and content
    # Note: these are still the defaults, we'll test the saved settings on the save tests
    changed_settings = sess.query(RuleSetting).filter(RuleSetting.priority.in_([1, 3])).all()
    for changed_setting in changed_settings:
        changed_setting.priority += 5
        changed_setting.impact_id = RULE_IMPACT_DICT['medium']
    sess.commit()
    rule_a1['significance'] = 2
    rule_a1['impact'] = 'medium'
    rule_a2['significance'] = 1
    rule_b1['significance'] = 2
    rule_b1['impact'] = 'medium'
    rule_b2['significance'] = 1

    results = rule_settings_endpoint(agency_code='1125', file='A')
    assert results['rules'] == [rule_a2, rule_a1]

    results = rule_settings_endpoint(agency_code='097', file='B')
    assert results['rules'] == [rule_b2, rule_b1]