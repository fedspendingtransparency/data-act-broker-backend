from dataactcore.models.domainModels import CGAC
from dataactcore.models.lookups import PERMISSION_TYPE_DICT
from dataactcore.models.userModel import User, UserAffiliation
from tests.unit.dataactcore.factories.domain import CGACFactory
from tests.unit.dataactcore.factories.user import UserFactory


def test_user_affiliation_fks(database, user_constants):
    sess = database.session
    users = [UserFactory() for _ in range(3)]
    cgacs = [CGACFactory() for _ in range(6)]
    permission = PERMISSION_TYPE_DICT["reader"]
    for idx, user in enumerate(users):
        user.affiliations = [
            UserAffiliation(cgac=cgacs[idx * 2], permission_type_id=permission),
            UserAffiliation(cgac=cgacs[idx * 2 + 1], permission_type_id=permission),
        ]
    sess.add_all(users)
    sess.commit()
    assert sess.query(UserAffiliation).count() == 6

    # Deleting a user also deletes the affiliations
    sess.delete(users[0])
    sess.commit()
    assert sess.query(UserAffiliation).count() == 4

    # Deleting a CGAC also deletes the affiliations
    sess.delete(cgacs[2])
    sess.commit()
    assert sess.query(UserAffiliation).count() == 3
    assert len(users[1].affiliations) == 1
    assert users[1].affiliations[0].cgac == cgacs[3]

    # Deleting an affiliation doesn't delete the user or CGAC
    assert sess.query(User).count() == 2
    assert sess.query(CGAC).count() == 5
    assert sess.query(UserAffiliation).count() == 3
    sess.delete(users[2].affiliations[0])
    sess.commit()
    assert sess.query(User).count() == 2
    assert sess.query(CGAC).count() == 5
    assert sess.query(UserAffiliation).count() == 2
