import json
import pytest
from sqlalchemy import and_

from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from dataactcore.models.lookups import FILE_TYPE_DICT_LETTER_ID, RULE_SEVERITY_DICT, RULE_IMPACT_DICT
from dataactcore.models.validationModels import RuleSql, RuleSetting
from dataactcore.models.domainModels import is_not_distinct_from
from dataactbroker.handlers import settings_handler
from dataactbroker.handlers.dashboard_handler import generate_file_type
from dataactcore.scripts.initialize import load_default_rule_settings
from dataactcore.utils.responseException import ResponseException


def rule_settings_endpoint(agency_code, file):
    json_response = settings_handler.list_rule_settings(agency_code=agency_code, file=file)
    assert json_response.status_code == 200
    return json.loads(json_response.get_data().decode('UTF-8'))


def save_rule_settings_endpoint(agency_code, file, errors, warnings):
    json_response = settings_handler.save_rule_settings(agency_code=agency_code, file=file, errors=errors,
                                                        warnings=warnings)
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
                      rule_cross_file_flag=False, target_file_id=None)
    rsql_a2 = RuleSql(rule_sql='', rule_label='A2', rule_error_message='A2 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                      rule_cross_file_flag=False, target_file_id=None)
    rsql_a3 = RuleSql(rule_sql='', rule_label='A3', rule_error_message='A3 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['warning'],
                      rule_cross_file_flag=False, target_file_id=None)
    rsql_a4 = RuleSql(rule_sql='', rule_label='A4', rule_error_message='A4 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                      rule_cross_file_flag=False, target_file_id=None)
    rsql_b1 = RuleSql(rule_sql='', rule_label='B1', rule_error_message='B1 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['B'], rule_severity_id=RULE_SEVERITY_DICT['warning'],
                      rule_cross_file_flag=False, target_file_id=None)
    rsql_b2 = RuleSql(rule_sql='', rule_label='B2', rule_error_message='B2 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['B'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                      rule_cross_file_flag=False, target_file_id=None)
    rsql_b3 = RuleSql(rule_sql='', rule_label='B3', rule_error_message='B3 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['B'], rule_severity_id=RULE_SEVERITY_DICT['warning'],
                      rule_cross_file_flag=False, target_file_id=None)
    rsql_b4 = RuleSql(rule_sql='', rule_label='B4', rule_error_message='B4 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['B'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                      rule_cross_file_flag=False, target_file_id=None)
    rsql_c1 = RuleSql(rule_sql='', rule_label='C1', rule_error_message='C1 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['C'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                      rule_cross_file_flag=False, target_file_id=None)
    rsql_c2 = RuleSql(rule_sql='', rule_label='C2', rule_error_message='C2 Description', query_name='',
                      file_id=FILE_TYPE_DICT_LETTER_ID['C'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                      rule_cross_file_flag=False, target_file_id=None)
    sess.add_all([rsql_a1, rsql_a2, rsql_a3, rsql_a4, rsql_b1, rsql_b2, rsql_b3, rsql_b4, rsql_c1, rsql_c2])

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
def test_agency_has_settings(database):
    """ Testing agency_has_settings function """
    sess = database.session
    setup_tests(sess)

    # Store some basic settings
    priorities = {'error': 1, 'warning': 1}
    rule_settings = []
    for rule in sess.query(RuleSql).filter(RuleSql.file_id == FILE_TYPE_DICT_LETTER_ID['A']).\
            order_by(RuleSql.rule_sql_id).all():

        if rule.rule_severity_id == RULE_SEVERITY_DICT['warning']:
            rule_settings.append(RuleSetting(rule_label=rule.rule_label, agency_code='1125',
                                             priority=priorities['warning'],
                                             impact_id=RULE_IMPACT_DICT['high'],
                                             file_id=rule.file_id, target_file_id=rule.target_file_id))
            priorities['warning'] += 1
        else:
            rule_settings.append(RuleSetting(rule_label=rule.rule_label, agency_code='1125',
                                             priority=priorities['error'],
                                             impact_id=RULE_IMPACT_DICT['high'],
                                             file_id=rule.file_id, target_file_id=rule.target_file_id))
            priorities['error'] += 1
    sess.add_all(rule_settings)
    sess.commit()

    assert settings_handler.agency_has_settings(sess=sess, agency_code='1125', file='A') is True
    assert settings_handler.agency_has_settings(sess=sess, agency_code='1125', file='B') is False
    assert settings_handler.agency_has_settings(sess=sess, agency_code='1124', file='A') is False


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
            cgac_rule_settings.append(RuleSetting(rule_label=rule.rule_label, agency_code='1125',
                                                  priority=priorities[file_type]['warning'],
                                                  impact_id=RULE_IMPACT_DICT['low'],
                                                  file_id=rule.file_id, target_file_id=rule.target_file_id))
            priorities[file_type]['warning'] -= 1
        else:
            cgac_rule_settings.append(RuleSetting(rule_label=rule.rule_label, agency_code='1125',
                                                  priority=priorities[file_type]['error'],
                                                  impact_id=RULE_IMPACT_DICT['medium'],
                                                  file_id=rule.file_id, target_file_id=rule.target_file_id))
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


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('validation_constants')
def test_save_rule_settings(database):
    """ Testing save_rule_settings function. """
    sess = database.session
    setup_tests(sess)

    def get_rule_settings_results(agency_code, file, rule_type):
        query = sess.query(RuleSetting.impact_id, RuleSql.rule_label).\
            join(RuleSql, and_(RuleSql.rule_label == RuleSetting.rule_label, RuleSql.file_id == RuleSetting.file_id,
                               is_not_distinct_from(RuleSql.target_file_id, RuleSetting.target_file_id))).\
            filter(RuleSql.file_id == FILE_TYPE_DICT_LETTER_ID[file], RuleSetting.agency_code == agency_code,
                   RuleSql.rule_severity_id == RULE_SEVERITY_DICT[rule_type]).\
            order_by(RuleSetting.priority)
        return query.all()

    # Normal results
    errors = [
        {'label': 'A4', 'impact': 'low'},
        {'label': 'A2', 'impact': 'high'}
    ]
    warnings = [
        {'label': 'A3', 'impact': 'high'},
        {'label': 'A1', 'impact': 'medium'}
    ]
    save_rule_settings_endpoint(agency_code='1125', file='A', errors=errors, warnings=warnings)

    a_error_settings = get_rule_settings_results('1125', 'A', 'fatal')
    assert a_error_settings[0].rule_label == 'A4'
    assert a_error_settings[0].impact_id == RULE_IMPACT_DICT['low']
    assert a_error_settings[1].rule_label == 'A2'
    assert a_error_settings[1].impact_id == RULE_IMPACT_DICT['high']
    a_warning_settings = get_rule_settings_results('1125', 'A', 'warning')
    assert a_warning_settings[0].rule_label == 'A3'
    assert a_warning_settings[0].impact_id == RULE_IMPACT_DICT['high']
    assert a_warning_settings[1].rule_label == 'A1'
    assert a_warning_settings[1].impact_id == RULE_IMPACT_DICT['medium']

    # Normal results - update that's already save
    errors = [
        {'label': 'A2', 'impact': 'medium'},
        {'label': 'A4', 'impact': 'low'}
    ]
    warnings = [
        {'label': 'A1', 'impact': 'low'},
        {'label': 'A3', 'impact': 'high'}
    ]
    save_rule_settings_endpoint(agency_code='1125', file='A', errors=errors, warnings=warnings)

    a_error_settings = get_rule_settings_results('1125', 'A', 'fatal')
    assert a_error_settings[0].rule_label == 'A2'
    assert a_error_settings[0].impact_id == RULE_IMPACT_DICT['medium']
    assert a_error_settings[1].rule_label == 'A4'
    assert a_error_settings[1].impact_id == RULE_IMPACT_DICT['low']
    a_warning_settings = get_rule_settings_results('1125', 'A', 'warning')
    assert a_warning_settings[0].rule_label == 'A1'
    assert a_warning_settings[0].impact_id == RULE_IMPACT_DICT['low']
    assert a_warning_settings[1].rule_label == 'A3'
    assert a_warning_settings[1].impact_id == RULE_IMPACT_DICT['high']

    # Testing the case if it still updates when there are errors and no warnings (visa versa)
    errors = [
        {'label': 'C2', 'impact': 'medium'},
        {'label': 'C1', 'impact': 'low'}
    ]
    warnings = []
    save_rule_settings_endpoint(agency_code='1125', file='C', errors=errors, warnings=warnings)

    c_error_settings = get_rule_settings_results('1125', 'C', 'fatal')
    assert c_error_settings[0].rule_label == 'C2'
    assert c_error_settings[0].impact_id == RULE_IMPACT_DICT['medium']
    assert c_error_settings[1].rule_label == 'C1'
    assert c_error_settings[1].impact_id == RULE_IMPACT_DICT['low']
    c_warning_settings = get_rule_settings_results('1125', 'C', 'warning')
    assert len(c_warning_settings) == 0

    # Failed results - not providing the rule error/warning lists
    errors = [
        {'label': 'A4', 'impact': 'low'}
    ]
    warnings = [
        {'label': 'A3', 'impact': 'high'},
        {'label': 'A1', 'impact': 'medium'}
    ]
    expected_error_text = 'Rules list provided doesn\'t match the rules expected: A4'
    with pytest.raises(ResponseException) as resp_except:
        save_rule_settings_endpoint(agency_code='1125', file='A', errors=errors, warnings=warnings)
    assert str(resp_except.value) == expected_error_text

    errors = [
        {'label': 'A4', 'impact': 'low'},
        {'label': 'A2', 'impact': 'high'}
    ]
    warnings = [
        {'label': 'A3', 'impact': 'high'}
    ]
    expected_error_text = 'Rules list provided doesn\'t match the rules expected: A3'
    with pytest.raises(ResponseException) as resp_except:
        save_rule_settings_endpoint(agency_code='1125', file='A', errors=errors, warnings=warnings)
    assert str(resp_except.value) == expected_error_text

    # Failed results - invalid rule dicts
    errors = [
        {'label': 'A4', 'impact': 'low'},
        {'label': 'A2', 'impact': 'high'}
    ]
    warnings = [
        {'label': 'A3', 'impact': 'high'},
        {'label': 'A1'}
    ]
    expected_error_text = 'Rule setting must have each of the following: '
    with pytest.raises(ResponseException) as resp_except:
        save_rule_settings_endpoint(agency_code='1125', file='A', errors=errors, warnings=warnings)
    assert str(resp_except.value).startswith(expected_error_text)

    errors = [
        {'label': 'A4', 'impact': 'low'},
        {'label': 'A2', 'impact': 'high'}
    ]
    warnings = [
        {'label': 'A3', 'impact': 'high'},
        {'impact': 'low'}
    ]
    expected_error_text = 'Rules list provided doesn\'t match the rules expected: A3'
    with pytest.raises(ResponseException) as resp_except:
        save_rule_settings_endpoint(agency_code='1125', file='A', errors=errors, warnings=warnings)
    assert str(resp_except.value).startswith(expected_error_text)

    # Failed results - invalid impacts
    errors = [
        {'label': 'A4', 'impact': '3'},
        {'label': 'A2', 'impact': 'high'}
    ]
    warnings = [
        {'label': 'A3', 'impact': 'high'},
        {'label': 'A1', 'impact': 'medium'}
    ]
    expected_error_text = 'Invalid impact: 3'
    with pytest.raises(ResponseException) as resp_except:
        save_rule_settings_endpoint(agency_code='1125', file='A', errors=errors, warnings=warnings)
    assert str(resp_except.value).startswith(expected_error_text)
