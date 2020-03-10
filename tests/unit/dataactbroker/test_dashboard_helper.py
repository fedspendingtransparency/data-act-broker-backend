import pytest

from dataactbroker.helpers import dashboard_helper
from dataactcore.models.validationModels import RuleSetting
from dataactcore.models.lookups import FILE_TYPE_DICT_LETTER_ID


def test_generate_file_type():
    # Normal
    assert dashboard_helper.generate_file_type(FILE_TYPE_DICT_LETTER_ID['A'], None) == 'A'
    assert dashboard_helper.generate_file_type(FILE_TYPE_DICT_LETTER_ID['B'], None) == 'B'
    assert dashboard_helper.generate_file_type(FILE_TYPE_DICT_LETTER_ID['C'], None) == 'C'
    assert dashboard_helper.generate_file_type(FILE_TYPE_DICT_LETTER_ID['D1'], None) == 'D1'
    assert dashboard_helper.generate_file_type(FILE_TYPE_DICT_LETTER_ID['D2'], None) == 'D2'
    # Cross
    assert dashboard_helper.generate_file_type(FILE_TYPE_DICT_LETTER_ID['A'],
                                               FILE_TYPE_DICT_LETTER_ID['B']) == 'cross-AB'
    assert dashboard_helper.generate_file_type(FILE_TYPE_DICT_LETTER_ID['B'],
                                               FILE_TYPE_DICT_LETTER_ID['C']) == 'cross-BC'
    assert dashboard_helper.generate_file_type(FILE_TYPE_DICT_LETTER_ID['C'],
                                               FILE_TYPE_DICT_LETTER_ID['D1']) == 'cross-CD1'
    assert dashboard_helper.generate_file_type(FILE_TYPE_DICT_LETTER_ID['C'],
                                               FILE_TYPE_DICT_LETTER_ID['D2']) == 'cross-CD2'
    # Bad
    assert dashboard_helper.generate_file_type(None, FILE_TYPE_DICT_LETTER_ID['D2']) is None


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('validation_constants')
def test_agency_has_settings(database):
    sess = database.session

    default = RuleSetting(file_id=FILE_TYPE_DICT_LETTER_ID['A'], target_file_id=None, agency_code=None,
                          priority=1, impact_id=1, rule_label='A4')
    setting_agency_a = RuleSetting(file_id=FILE_TYPE_DICT_LETTER_ID['A'], target_file_id=None, agency_code='001',
                                   priority=1, impact_id=1, rule_label='A4')
    setting_agency_b = RuleSetting(file_id=FILE_TYPE_DICT_LETTER_ID['A'], target_file_id=None, agency_code='002',
                                   priority=1, impact_id=1, rule_label='A4')

    sess.add_all([default, setting_agency_a])
    assert dashboard_helper.agency_has_settings(sess, '002', 'A') is False

    sess.add_all([setting_agency_b])
    assert dashboard_helper.agency_has_settings(sess, '002', 'A') is True
    assert dashboard_helper.agency_has_settings(sess, '002', 'B') is False


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('validation_constants')
def test_agency_settings_filter(database):
    sess = database.session

    default_setting = RuleSetting(file_id=FILE_TYPE_DICT_LETTER_ID['A'], target_file_id=None, agency_code=None,
                                  priority=1, impact_id=1, rule_label='A3')
    agency_setting = RuleSetting(file_id=FILE_TYPE_DICT_LETTER_ID['A'], target_file_id=None, agency_code='001',
                                 priority=1, impact_id=1, rule_label='A4')

    base_query = sess.query(RuleSetting)
    sess.add_all([default_setting, agency_setting])
    assert dashboard_helper.agency_settings_filter(sess, base_query, '000', 'A').first() == default_setting
    assert dashboard_helper.agency_settings_filter(sess, base_query, '001', 'A').first() == agency_setting
