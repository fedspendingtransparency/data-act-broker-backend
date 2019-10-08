from tests.unit.dataactcore.factories.user import UserFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.userModel import User


def test_delete_user(database):
    sess = GlobalDB.db().session
    # Create user with email
    user_to_be_deleted = UserFactory()
    email = user_to_be_deleted.email
    other_user = UserFactory()
    database.session.add_all([user_to_be_deleted, other_user])
    database.session.commit()
    # Create two submissions for this user and one for a different user
    sub_one = SubmissionFactory(user_id=user_to_be_deleted.user_id)
    sub_two = SubmissionFactory(user_id=user_to_be_deleted.user_id)
    other_sub = SubmissionFactory(user_id=other_user.user_id)
    database.session.add_all([sub_one, sub_two, other_sub])
    database.session.commit()
    # Delete a user
    sess.query(User).filter(User.email == email).delete(synchronize_session='fetch')
    sess.commit()
    # Confirm user has been deleted and that user's submissions have no user_id
    assert database.session.query(User).filter_by(email=email).count() == 0
    assert sub_one.user_id is None
    assert sub_two.user_id is None
    # Confirm that other submission was not affected
    assert other_sub.user_id == other_user.user_id
