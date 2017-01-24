from unittest.mock import Mock

from flask import g
import pytest

from dataactbroker import permissions
from dataactcore.models.lookups import PERMISSION_TYPE_DICT
from dataactcore.models.userModel import UserAffiliation
from dataactcore.utils.responseException import ResponseException
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.user import UserFactory


def test_current_user_can(database, monkeypatch, user_constants):
    user_cgac, other_cgac = [CGACFactory() for _ in range(2)]
    user = UserFactory(affiliations=[
        UserAffiliation(cgac=user_cgac, permission_type_id=PERMISSION_TYPE_DICT['writer'])
    ])
    database.session.add_all([user_cgac, other_cgac, user])
    database.session.commit()

    monkeypatch.setattr(permissions, 'g', Mock(user=user))

    # has permission level, but wrong agency
    assert not permissions.current_user_can('reader', other_cgac.cgac_code)
    # has agency, but not permission level
    assert not permissions.current_user_can('submitter', user_cgac.cgac_code)
    # right agency, right permission
    assert permissions.current_user_can('writer', user_cgac.cgac_code)
    assert permissions.current_user_can('reader', user_cgac.cgac_code)
    # wrong permission level, wrong agency, but superuser
    user.website_admin = True
    assert permissions.current_user_can('submitter', user_cgac.cgac_code)


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
