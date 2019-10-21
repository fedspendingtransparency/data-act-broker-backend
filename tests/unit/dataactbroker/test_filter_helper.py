import pytest
from unittest.mock import Mock

from dataactbroker.helpers import filters_helper
from dataactcore.models.jobModels import Submission
from dataactcore.models.lookups import PERMISSION_TYPE_DICT
from dataactcore.models.userModel import UserAffiliation
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
