from unittest.mock import Mock

from flask import g
import pytest

from dataactbroker import permissions
from dataactcore.models.lookups import ALL_PERMISSION_TYPES_DICT, PERMISSION_TYPE_DICT
from dataactcore.models.userModel import UserAffiliation
from dataactcore.utils.responseException import ResponseException
from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.user import UserFactory


def test_current_user_can_dabs_cgac_reader(database, monkeypatch, user_constants):
    user_cgac, other_cgac = [CGACFactory() for _ in range(2)]
    user_reader = UserFactory(affiliations=[
        UserAffiliation(cgac=user_cgac, permission_type_id=PERMISSION_TYPE_DICT['reader'])
    ])
    database.session.add_all([user_cgac, other_cgac, user_reader])
    database.session.commit()

    monkeypatch.setattr(permissions, 'g', Mock(user=user_reader))

    # has permission level, but wrong agency
    assert not permissions.current_user_can('reader', other_cgac.cgac_code, None)

    # has agency, but not permission level
    assert not permissions.current_user_can('writer', user_cgac.cgac_code, None)
    assert not permissions.current_user_can('submitter', user_cgac.cgac_code, None)
    assert not permissions.current_user_can('editfabs', user_cgac.cgac_code, None)
    assert not permissions.current_user_can('fabs', user_cgac.cgac_code, None)

    # right agency, right permission
    assert permissions.current_user_can('reader', user_cgac.cgac_code, None)

    # wrong permission level, wrong agency, but superuser
    user_reader.website_admin = True
    assert permissions.current_user_can('submitter', other_cgac.cgac_code, None)


def test_current_user_can_dabs_cgac_writer(database, monkeypatch, user_constants):
    user_cgac, other_cgac = [CGACFactory() for _ in range(2)]
    user_writer = UserFactory(affiliations=[
        UserAffiliation(cgac=user_cgac, permission_type_id=PERMISSION_TYPE_DICT['writer'])
    ])
    database.session.add_all([user_cgac, other_cgac, user_writer])
    database.session.commit()

    monkeypatch.setattr(permissions, 'g', Mock(user=user_writer))

    # has permission level, but wrong agency
    assert not permissions.current_user_can('reader', other_cgac.cgac_code, None)
    assert not permissions.current_user_can('writer', other_cgac.cgac_code, None)

    # has agency, but not permission level
    assert not permissions.current_user_can('submitter', user_cgac.cgac_code, None)
    assert not permissions.current_user_can('editfabs', user_cgac.cgac_code, None)
    assert not permissions.current_user_can('fabs', user_cgac.cgac_code, None)

    # right agency, right permission
    assert permissions.current_user_can('reader', user_cgac.cgac_code, None)
    assert permissions.current_user_can('writer', user_cgac.cgac_code, None)

    # wrong permission level, wrong agency, but superuser
    user_writer.website_admin = True
    assert permissions.current_user_can('submitter', other_cgac.cgac_code, None)


def test_current_user_can_dabs_cgac_submitter(database, monkeypatch, user_constants):
    user_cgac, other_cgac = [CGACFactory() for _ in range(2)]
    user_submitter = UserFactory(affiliations=[
        UserAffiliation(cgac=user_cgac, permission_type_id=PERMISSION_TYPE_DICT['submitter'])
    ])
    database.session.add_all([user_cgac, other_cgac, user_submitter])
    database.session.commit()

    monkeypatch.setattr(permissions, 'g', Mock(user=user_submitter))

    # has permission level, but wrong agency
    assert not permissions.current_user_can('reader', other_cgac.cgac_code, None)
    assert not permissions.current_user_can('writer', other_cgac.cgac_code, None)
    assert not permissions.current_user_can('submitter', other_cgac.cgac_code, None)

    # has agency, but not permission level
    assert not permissions.current_user_can('editfabs', user_cgac.cgac_code, None)
    assert not permissions.current_user_can('fabs', user_cgac.cgac_code, None)

    # right agency, right permission
    assert permissions.current_user_can('reader', user_cgac.cgac_code, None)
    assert permissions.current_user_can('writer', user_cgac.cgac_code, None)
    assert permissions.current_user_can('submitter', user_cgac.cgac_code, None)

    # wrong agency, but superuser
    user_submitter.website_admin = True
    assert permissions.current_user_can('submitter', other_cgac.cgac_code, None)


def test_current_user_can_dabs_frec_reader(database, monkeypatch, user_constants):
    user_cgac, other_cgac = [CGACFactory() for _ in range(2)]
    user_frec, other_frec = FRECFactory(cgac=user_cgac), FRECFactory(cgac=other_cgac)
    user_reader = UserFactory(affiliations=[
        UserAffiliation(frec=user_frec, permission_type_id=PERMISSION_TYPE_DICT['reader'])
    ])
    database.session.add_all([user_cgac, other_cgac, user_frec, other_frec, user_reader])
    database.session.commit()

    monkeypatch.setattr(permissions, 'g', Mock(user=user_reader))

    # has permission level, but wrong agency
    assert not permissions.current_user_can('reader', None, other_frec.frec_code)

    # has agency, but not permission level
    assert not permissions.current_user_can('writer', None, user_frec.frec_code)
    assert not permissions.current_user_can('submitter', None, user_frec.frec_code)
    assert not permissions.current_user_can('editfabs', user_frec.frec_code, None)
    assert not permissions.current_user_can('fabs', user_frec.frec_code, None)

    # right agency, right permission
    assert permissions.current_user_can('reader', None, user_frec.frec_code)

    # wrong permission level, wrong agency, but superuser
    user_reader.website_admin = True
    assert permissions.current_user_can('submitter', None, other_frec.frec_code)


def test_current_user_can_dabs_frec_writer(database, monkeypatch, user_constants):
    user_cgac, other_cgac = [CGACFactory() for _ in range(2)]
    user_frec, other_frec = FRECFactory(cgac=user_cgac), FRECFactory(cgac=other_cgac)
    user_writer = UserFactory(affiliations=[
        UserAffiliation(frec=user_frec, permission_type_id=PERMISSION_TYPE_DICT['writer'])
    ])
    database.session.add_all([user_cgac, other_cgac, user_frec, other_frec, user_writer])
    database.session.commit()

    monkeypatch.setattr(permissions, 'g', Mock(user=user_writer))

    # has permission level, but wrong agency
    assert not permissions.current_user_can('reader', None, other_frec.frec_code)
    assert not permissions.current_user_can('writer', None, other_frec.frec_code)

    # has agency, but not permission level
    assert not permissions.current_user_can('submitter', None, user_frec.frec_code)
    assert not permissions.current_user_can('editfabs', user_frec.frec_code, None)
    assert not permissions.current_user_can('fabs', user_frec.frec_code, None)

    # right agency, right permission
    assert permissions.current_user_can('reader', None, user_frec.frec_code)
    assert permissions.current_user_can('writer', None, user_frec.frec_code)

    # wrong permission level, wrong agency, but superuser
    user_writer.website_admin = True
    assert permissions.current_user_can('submitter', None, other_frec.frec_code)


def test_current_user_can_dabs_frec_submitter(database, monkeypatch, user_constants):
    user_cgac, other_cgac = [CGACFactory() for _ in range(2)]
    user_frec, other_frec = FRECFactory(cgac=user_cgac), FRECFactory(cgac=other_cgac)
    user_submitter = UserFactory(affiliations=[
        UserAffiliation(frec=user_frec, permission_type_id=PERMISSION_TYPE_DICT['submitter'])
    ])
    database.session.add_all([user_cgac, other_cgac, user_frec, other_frec, user_submitter])
    database.session.commit()

    monkeypatch.setattr(permissions, 'g', Mock(user=user_submitter))

    # has permission level, but wrong agency
    assert not permissions.current_user_can('reader', None, other_frec.frec_code)
    assert not permissions.current_user_can('writer', None, other_frec.frec_code)
    assert not permissions.current_user_can('submitter', None, other_frec.frec_code)

    # has agency, but not permission level
    assert not permissions.current_user_can('editfabs', user_frec.frec_code, None)
    assert not permissions.current_user_can('fabs', user_frec.frec_code, None)

    # right agency, right permission
    assert permissions.current_user_can('reader', None, user_frec.frec_code)
    assert permissions.current_user_can('writer', None, user_frec.frec_code)
    assert permissions.current_user_can('submitter', None, user_frec.frec_code)

    # wrong agency, but superuser
    user_submitter.website_admin = True
    assert permissions.current_user_can('submitter', None, other_frec.frec_code)


def test_current_user_can_fabs_cgac_editfabs(database, monkeypatch, user_constants):
    user_cgac, other_cgac = [CGACFactory() for _ in range(2)]
    user_editfabs = UserFactory(affiliations=[
        UserAffiliation(cgac=user_cgac, permission_type_id=ALL_PERMISSION_TYPES_DICT['editfabs'])
    ])
    database.session.add_all([user_cgac, other_cgac, user_editfabs])
    database.session.commit()

    monkeypatch.setattr(permissions, 'g', Mock(user=user_editfabs))

    # has permission level, but wrong agency
    assert not permissions.current_user_can('reader', other_cgac.cgac_code, None)
    assert not permissions.current_user_can('editfabs', other_cgac.cgac_code, None)

    # has agency, but not permission level
    assert not permissions.current_user_can('writer', user_cgac.cgac_code, None)
    assert not permissions.current_user_can('submitter', user_cgac.cgac_code, None)
    assert not permissions.current_user_can('fabs', user_cgac.cgac_code, None)

    # right agency, right permission
    assert permissions.current_user_can('reader', user_cgac.cgac_code, None)
    assert permissions.current_user_can('editfabs', user_cgac.cgac_code, None)

    # wrong permission level, wrong agency, but superuser
    user_editfabs.website_admin = True
    assert permissions.current_user_can('fabs', other_cgac.cgac_code, None)


def test_current_user_can_fabs_cgac_fabs(database, monkeypatch, user_constants):
    user_cgac, other_cgac = [CGACFactory() for _ in range(2)]
    user_fabs = UserFactory(affiliations=[
        UserAffiliation(cgac=user_cgac, permission_type_id=ALL_PERMISSION_TYPES_DICT['fabs'])
    ])
    database.session.add_all([user_cgac, other_cgac, user_fabs])
    database.session.commit()

    monkeypatch.setattr(permissions, 'g', Mock(user=user_fabs))

    # has permission level, but wrong agency
    assert not permissions.current_user_can('reader', other_cgac.cgac_code, None)
    assert not permissions.current_user_can('editfabs', other_cgac.cgac_code, None)
    assert not permissions.current_user_can('fabs', other_cgac.cgac_code, None)

    # has agency, but not permission level
    assert not permissions.current_user_can('writer', user_cgac.cgac_code, None)
    assert not permissions.current_user_can('submitter', user_cgac.cgac_code, None)

    # right agency, right permission
    assert permissions.current_user_can('reader', user_cgac.cgac_code, None)
    assert permissions.current_user_can('editfabs', user_cgac.cgac_code, None)
    assert permissions.current_user_can('fabs', user_cgac.cgac_code, None)

    # wrong agency, but superuser
    user_fabs.website_admin = True
    assert permissions.current_user_can('fabs', other_cgac.cgac_code, None)


def test_current_user_can_fabs_frec_editfabs(database, monkeypatch, user_constants):
    user_cgac, other_cgac = [CGACFactory() for _ in range(2)]
    user_frec, other_frec = FRECFactory(cgac=user_cgac), FRECFactory(cgac=other_cgac)
    user_editfabs = UserFactory(affiliations=[
        UserAffiliation(frec=user_frec, permission_type_id=ALL_PERMISSION_TYPES_DICT['editfabs'])
    ])
    database.session.add_all([user_cgac, other_cgac, user_frec, other_frec, user_editfabs])
    database.session.commit()

    monkeypatch.setattr(permissions, 'g', Mock(user=user_editfabs))

    # has permission level, but wrong agency
    assert not permissions.current_user_can('reader', None, other_frec.frec_code)
    assert not permissions.current_user_can('editfabs', None, other_frec.frec_code)

    # has agency, but not permission level
    assert not permissions.current_user_can('writer', None, user_frec.frec_code)
    assert not permissions.current_user_can('submitter', None, user_frec.frec_code)
    assert not permissions.current_user_can('fabs', None, user_frec.frec_code)

    # right agency, right permission
    assert permissions.current_user_can('reader', None, user_frec.frec_code)
    assert permissions.current_user_can('editfabs', None, user_frec.frec_code)

    # wrong permission level, wrong agency, but superuser
    user_editfabs.website_admin = True
    assert permissions.current_user_can('fabs', None, other_frec.frec_code)


def test_current_user_can_fabs_frec_fabs(database, monkeypatch, user_constants):
    user_cgac, other_cgac = [CGACFactory() for _ in range(2)]
    user_frec, other_frec = FRECFactory(cgac=user_cgac), FRECFactory(cgac=other_cgac)
    user_fabs = UserFactory(affiliations=[
        UserAffiliation(frec=user_frec, permission_type_id=ALL_PERMISSION_TYPES_DICT['fabs'])
    ])
    database.session.add_all([user_cgac, other_cgac, user_frec, other_frec, user_fabs])
    database.session.commit()

    monkeypatch.setattr(permissions, 'g', Mock(user=user_fabs))

    # has permission level, but wrong agency
    assert not permissions.current_user_can('reader', None, other_frec.frec_code)
    assert not permissions.current_user_can('editfabs', None, other_frec.frec_code)
    assert not permissions.current_user_can('fabs', None, other_frec.frec_code)

    # has agency, but not permission level
    assert not permissions.current_user_can('writer', None, user_frec.frec_code)
    assert not permissions.current_user_can('submitter', None, user_frec.frec_code)

    # right agency, right permission
    assert permissions.current_user_can('reader', None, user_frec.frec_code)
    assert permissions.current_user_can('editfabs', None, user_frec.frec_code)
    assert permissions.current_user_can('fabs', None, user_frec.frec_code)

    # wrong agency, but superuser
    user_fabs.website_admin = True
    assert permissions.current_user_can('fabs', None, other_frec.frec_code)


def test_current_user_can_multiple_fabs_permissions(database, monkeypatch, user_constants):
    user_cgac, other_cgac = [CGACFactory() for _ in range(2)]
    user_fabs = UserFactory(affiliations=[
        UserAffiliation(cgac=user_cgac, permission_type_id=ALL_PERMISSION_TYPES_DICT['editfabs']),
        UserAffiliation(cgac=user_cgac, permission_type_id=ALL_PERMISSION_TYPES_DICT['fabs'])
    ])
    database.session.add_all([user_cgac, other_cgac, user_fabs])
    database.session.commit()

    monkeypatch.setattr(permissions, 'g', Mock(user=user_fabs))

    # has permission level, but wrong agency
    assert not permissions.current_user_can('reader', other_cgac.cgac_code, None)
    assert not permissions.current_user_can('editfabs', other_cgac.cgac_code, None)
    assert not permissions.current_user_can('fabs', other_cgac.cgac_code, None)

    # has agency, but not permission level
    assert not permissions.current_user_can('writer', user_cgac.cgac_code, None)
    assert not permissions.current_user_can('submitter', user_cgac.cgac_code, None)

    # right agency, right permission
    assert permissions.current_user_can('reader', user_cgac.cgac_code, None)
    assert permissions.current_user_can('editfabs', user_cgac.cgac_code, None)
    assert permissions.current_user_can('fabs', user_cgac.cgac_code, None)

    # wrong agency, but superuser
    user_fabs.website_admin = True
    assert permissions.current_user_can('fabs', other_cgac.cgac_code, None)


def test_current_user_can_multiple_dabs_permissions(database, monkeypatch, user_constants):
    user_cgac, other_cgac = [CGACFactory() for _ in range(2)]
    user_submitter = UserFactory(affiliations=[
        UserAffiliation(cgac=user_cgac, permission_type_id=PERMISSION_TYPE_DICT['writer']),
        UserAffiliation(cgac=user_cgac, permission_type_id=PERMISSION_TYPE_DICT['submitter'])
    ])
    database.session.add_all([user_cgac, other_cgac, user_submitter])
    database.session.commit()

    monkeypatch.setattr(permissions, 'g', Mock(user=user_submitter))

    # has permission level, but wrong agency
    assert not permissions.current_user_can('reader', other_cgac.cgac_code, None)
    assert not permissions.current_user_can('writer', other_cgac.cgac_code, None)
    assert not permissions.current_user_can('submitter', other_cgac.cgac_code, None)

    # has agency, but not permission level
    assert not permissions.current_user_can('editfabs', user_cgac.cgac_code, None)
    assert not permissions.current_user_can('fabs', user_cgac.cgac_code, None)

    # right agency, right permission
    assert permissions.current_user_can('reader', user_cgac.cgac_code, None)
    assert permissions.current_user_can('writer', user_cgac.cgac_code, None)
    assert permissions.current_user_can('submitter', user_cgac.cgac_code, None)

    # wrong agency, but superuser
    user_submitter.website_admin = True
    assert permissions.current_user_can('submitter', other_cgac.cgac_code, None)


def test_current_user_can_on_submission(monkeypatch, database):
    submission = SubmissionFactory()
    user = UserFactory()
    database.session.add_all([submission, user])
    database.session.commit()

    current_user_can = Mock()
    monkeypatch.setattr(permissions, 'g', Mock(user=user))
    monkeypatch.setattr(permissions, 'current_user_can', current_user_can)

    current_user_can.return_value = True
    assert permissions.current_user_can_on_submission('reader', submission)
    current_user_can.return_value = False
    assert not permissions.current_user_can_on_submission('reader', submission)
    submission.user_id = user.user_id
    assert permissions.current_user_can_on_submission('reader', submission)


def test_requires_submission_perm_no_submission(database, test_app):
    """If no submission exists, we should see an exception"""
    sub = SubmissionFactory(user=UserFactory())
    database.session.add(sub)
    database.session.commit()
    g.user = sub.user

    fn = permissions.requires_submission_perms('writer')(Mock())
    # Does not raise exception
    fn(sub.submission_id)
    with pytest.raises(ResponseException):
        fn(sub.submission_id + 1)   # different submission id
