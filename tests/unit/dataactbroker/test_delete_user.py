from dataactbroker.handlers.userHandler import UserHandler
from tests.unit.dataactcore.factories.user import UserFactory
from tests.unit.dataactcore.factories.job import SubmissionFactory
from dataactcore.models.userModel import User
from dataactcore.models.jobModels import Submission

def test_delete_user(database):
    user_handler = UserHandler()
    # Create user with email
    user_to_be_deleted = UserFactory()
    email = user_to_be_deleted.email
    other_user = UserFactory()
    database.session.add(user_to_be_deleted)
    database.session.add(other_user)
    database.session.commit()
    # Create two submissions for this user and one for a different user
    sub_one = SubmissionFactory(user_id = user_to_be_deleted.user_id)
    sub_two = SubmissionFactory(user_id = user_to_be_deleted.user_id)
    other_sub = SubmissionFactory(user_id = other_user.user_id)
    database.session.add(sub_one)
    database.session.add(sub_two)
    database.session.add(other_sub)
    database.session.commit()
    # Delete a user
    user_handler.deleteUser(email)
    # Confirm user has been deleted and that user's submissions have no user_id
    assert(len(database.session.query(User).filter(User.email == email).all()) == 0)
    assert(sub_one.user_id is None)
    assert(sub_two.user_id is None)
    # Confirm that other submission was not affected
    assert(other_sub.user_id == other_user.user_id)