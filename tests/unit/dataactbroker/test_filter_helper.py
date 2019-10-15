import pytest
from unittest.mock import Mock

from dataactbroker.helpers import filters_helper
from dataactcore.models.errorModels import CertifiedErrorMetadata
from dataactcore.models.jobModels import Submission
from dataactcore.models.lookups import PERMISSION_TYPE_DICT, FILE_TYPE_DICT_LETTER_ID, RULE_SEVERITY_DICT
from dataactcore.models.userModel import UserAffiliation
from dataactcore.models.validationModels import RuleSql
from dataactcore.utils.responseException import ResponseException
from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.user import UserFactory


def test_agency_filter(database):
    sess = database.session
    db_objects = []

    # Setup agencies
    cgac1 = CGACFactory(cgac_code='089', agency_name='CGAC')
    cgac2 = CGACFactory(cgac_code='011', agency_name='CGAC Associated with FREC')
    cgac3 = CGACFactory(cgac_code='091', agency_name='Other CGAC Associated with FREC')
    frec1 = FRECFactory(cgac=cgac2, frec_code='1125', agency_name='FREC 1')
    frec2 = FRECFactory(cgac=cgac3, frec_code='0923', agency_name='FREC 2')
    db_objects.extend([cgac1, cgac2, cgac3, frec1, frec2])

    # Setup submissions
    sub1 = SubmissionFactory(cgac_code=cgac1.cgac_code, frec_code=None)
    sub2 = SubmissionFactory(cgac_code=cgac2.cgac_code, frec_code=frec1.frec_code)
    sub3 = SubmissionFactory(cgac_code=cgac3.cgac_code, frec_code=None)
    sub4 = SubmissionFactory(cgac_code=cgac3.cgac_code, frec_code=frec2.frec_code)
    db_objects.extend([sub1, sub2, sub3, sub4])

    sess.add_all(db_objects)
    sess.commit()

    base_query = sess.query(Submission)

    # no agency list, no filtering
    agency_list = []
    query = filters_helper.agency_filter(sess, base_query, cgac_model=Submission, frec_model=Submission,
                                         agency_list=agency_list)
    expected_results = [sub1, sub2, sub3, sub4]
    results = query.all()
    assert set(results) == set(expected_results)

    # filter for CGACS
    agency_list = ['011']
    query = filters_helper.agency_filter(sess, base_query, cgac_model=Submission, frec_model=Submission,
                                         agency_list=agency_list)
    expected_results = [sub2]
    results = list(query.all())
    assert results == expected_results

    # filter for FRECS
    agency_list = ['0923']
    query = filters_helper.agency_filter(sess, base_query, cgac_model=Submission, frec_model=Submission,
                                         agency_list=agency_list)
    expected_results = [sub4]
    results = list(query.all())
    assert results == expected_results

    # filter for both
    agency_list = ['011', '0923']
    query = filters_helper.agency_filter(sess, base_query, cgac_model=Submission, frec_model=Submission,
                                         agency_list=agency_list)
    expected_results = [sub2, sub4]
    results = set(query.all())
    assert results == set(expected_results)

    # throw in one that doesn't fit the agency format
    agency_list = ['089', '1125', '3']
    expected_response = 'All codes in the agency_codes filter must be valid agency codes'
    with pytest.raises(ResponseException) as resp_except:
        filters_helper.agency_filter(sess, base_query, cgac_model=Submission, frec_model=Submission,
                                     agency_list=agency_list)
    assert resp_except.value.status == 400
    assert str(resp_except.value) == expected_response

    # throw in one that just doesn't exist
    agency_list = ['089', '1125', '012']
    with pytest.raises(ResponseException) as resp_except:
        filters_helper.agency_filter(sess, base_query, cgac_model=Submission, frec_model=Submission,
                                     agency_list=agency_list)
    assert resp_except.value.status == 400
    assert str(resp_except.value) == expected_response


def test_permissions_filter_admin(database, monkeypatch):
    sess = database.session
    db_objects = []

    # Setup admin user
    admin_user = UserFactory(name='Administrator', website_admin=True)
    db_objects.append(admin_user)
    monkeypatch.setattr(filters_helper, 'g', Mock(user=admin_user))

    # admin user queries should be identical to the provided query
    base_query = sess.query(Submission)
    query = filters_helper.permissions_filter(base_query)
    assert query == base_query


@pytest.mark.usefixtures("user_constants")
def test_permissions_filter_agency_user(database, monkeypatch):
    sess = database.session

    # Setup agencies
    db_objects = []
    cgac1 = CGACFactory(cgac_code='089', agency_name='CGAC')
    cgac2 = CGACFactory(cgac_code='011', agency_name='CGAC Associated with FREC')
    cgac3 = CGACFactory(cgac_code='091', agency_name='Other CGAC Associated with FREC')
    frec1 = FRECFactory(cgac=cgac2, frec_code='1125', agency_name='FREC 1')
    frec2 = FRECFactory(cgac=cgac3, frec_code='0923', agency_name='FREC 2')
    db_objects.extend([cgac1, cgac2, cgac3, frec1, frec2])

    # Setup submissions
    sub1 = SubmissionFactory(cgac_code=cgac1.cgac_code, frec_code=None)
    sub2 = SubmissionFactory(cgac_code=cgac2.cgac_code, frec_code=frec1.frec_code)
    sub3 = SubmissionFactory(cgac_code=cgac3.cgac_code, frec_code=None)
    sub4 = SubmissionFactory(cgac_code=cgac3.cgac_code, frec_code=frec2.frec_code)
    db_objects.extend([sub1, sub2, sub3, sub4])

    # Setup agency user
    agency_user = UserFactory(name='Agency User', affiliations=[
        UserAffiliation(user_affiliation_id=1, cgac=cgac1, permission_type_id=PERMISSION_TYPE_DICT['reader']),
        UserAffiliation(user_affiliation_id=2, cgac=cgac2, frec=frec1,
                        permission_type_id=PERMISSION_TYPE_DICT['writer']),
    ])
    db_objects.append(agency_user)
    monkeypatch.setattr(filters_helper, 'g', Mock(user=agency_user))

    sess.add_all(db_objects)
    sess.commit()

    base_query = sess.query(Submission)

    # submissions should be filtered based on user access
    query = filters_helper.permissions_filter(base_query)
    expected_results = [sub1, sub2]
    results = set(query.all())
    assert results == set(expected_results)

@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('validation_constants')
def test_file_filter_rulesql(database):
    sess = database.session

    # Setup RuleSql
    rsql_a = RuleSql(rule_sql='', rule_label='A1', rule_error_message='', query_name='',
                     file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                     rule_cross_file_flag=False, target_file_id=None)
    rsql_b = RuleSql(rule_sql='', rule_label='B2', rule_error_message='', query_name='',
                     file_id=FILE_TYPE_DICT_LETTER_ID['B'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                     rule_cross_file_flag=False, target_file_id=None)
    rsql_c = RuleSql(rule_sql='', rule_label='C3', rule_error_message='', query_name='',
                     file_id=FILE_TYPE_DICT_LETTER_ID['C'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                     rule_cross_file_flag=False, target_file_id=None)
    rsql_cross_ab = RuleSql(rule_sql='', rule_label='A4', rule_error_message='', query_name='',
                            file_id=FILE_TYPE_DICT_LETTER_ID['A'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                            rule_cross_file_flag=True, target_file_id=FILE_TYPE_DICT_LETTER_ID['B'])
    rsql_cross_ba = RuleSql(rule_sql='', rule_label='B5', rule_error_message='', query_name='',
                            file_id=FILE_TYPE_DICT_LETTER_ID['B'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                            rule_cross_file_flag=True, target_file_id=FILE_TYPE_DICT_LETTER_ID['A'])
    rsql_cross_bc = RuleSql(rule_sql='', rule_label='B6', rule_error_message='', query_name='',
                            file_id=FILE_TYPE_DICT_LETTER_ID['B'], rule_severity_id=RULE_SEVERITY_DICT['fatal'],
                            rule_cross_file_flag=True, target_file_id=FILE_TYPE_DICT_LETTER_ID['C'])
    all_rules = [rsql_a, rsql_b, rsql_c, rsql_cross_ab, rsql_cross_ba, rsql_cross_bc]
    sess.add_all(all_rules)
    sess.commit()

    base_query = sess.query(RuleSql)

    # no file list, no filtering
    files = []
    query = filters_helper.file_filter(base_query, RuleSql, files)
    expected_results = all_rules
    results = query.all()
    assert set(results) == set(expected_results)

    # filter by single file
    files = ['A', 'C']
    query = filters_helper.file_filter(base_query, RuleSql, files)
    expected_results = [rsql_a, rsql_c]
    results = query.all()
    assert set(results) == set(expected_results)

    # filter by cross file
    files = ['cross-AB']
    query = filters_helper.file_filter(base_query, RuleSql, files)
    expected_results = [rsql_cross_ab, rsql_cross_ba]
    results = query.all()
    assert set(results) == set(expected_results)


@pytest.mark.usefixtures('job_constants')
@pytest.mark.usefixtures('validation_constants')
def test_file_filter_cert_error_metadata(database):
    sess = database.session

    # Setup CertifiedErrorMetadata
    cem_a = CertifiedErrorMetadata(original_rule_label='A1', file_type_id=FILE_TYPE_DICT_LETTER_ID['A'],
                                   severity_id=RULE_SEVERITY_DICT['fatal'], target_file_type_id=None)
    cem_b = CertifiedErrorMetadata(original_rule_label='B2', file_type_id=FILE_TYPE_DICT_LETTER_ID['B'],
                                   severity_id=RULE_SEVERITY_DICT['fatal'], target_file_type_id=None)
    cem_c = CertifiedErrorMetadata(original_rule_label='C3', file_type_id=FILE_TYPE_DICT_LETTER_ID['C'],
                                   severity_id=RULE_SEVERITY_DICT['fatal'], target_file_type_id=None)
    cem_cross_ab = CertifiedErrorMetadata(original_rule_label='A4', file_type_id=FILE_TYPE_DICT_LETTER_ID['A'],
                                          severity_id=RULE_SEVERITY_DICT['fatal'],
                                          target_file_type_id=FILE_TYPE_DICT_LETTER_ID['B'])
    cem_cross_ba = CertifiedErrorMetadata(original_rule_label='B5', file_type_id=FILE_TYPE_DICT_LETTER_ID['B'],
                                          severity_id=RULE_SEVERITY_DICT['fatal'],
                                          target_file_type_id=FILE_TYPE_DICT_LETTER_ID['A'])
    cem_cross_bc = CertifiedErrorMetadata(original_rule_label='B6', file_type_id=FILE_TYPE_DICT_LETTER_ID['B'],
                                          severity_id=RULE_SEVERITY_DICT['fatal'],
                                          target_file_type_id=FILE_TYPE_DICT_LETTER_ID['C'])
    all_cems = [cem_a, cem_b, cem_c, cem_cross_ab, cem_cross_ba, cem_cross_bc]
    sess.add_all(all_cems)
    sess.commit()

    base_query = sess.query(CertifiedErrorMetadata)

    # no file list, no filtering
    files = []
    query = filters_helper.file_filter(base_query, CertifiedErrorMetadata, files)
    expected_results = all_cems
    results = query.all()
    assert set(results) == set(expected_results)

    # filter by single file
    files = ['A', 'C']
    query = filters_helper.file_filter(base_query, CertifiedErrorMetadata, files)
    expected_results = [cem_a, cem_c]
    results = query.all()
    assert set(results) == set(expected_results)

    # filter by cross file
    files = ['cross-AB']
    query = filters_helper.file_filter(base_query, CertifiedErrorMetadata, files)
    expected_results = [cem_cross_ab, cem_cross_ba]
    results = query.all()
    assert set(results) == set(expected_results)
