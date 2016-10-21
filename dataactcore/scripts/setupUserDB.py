from dataactbroker.app import createApp
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models import lookups
from dataactcore.models.userModel import PermissionType, UserStatus


def setupUserDB():
    """Create user tables from model metadata."""
    with createApp().app_context():
        sess = GlobalDB.db().session
        insertCodes(sess)
        sess.commit()


def insertCodes(sess):
    """Create job tracker tables from model metadata."""
    # insert status types
    for s in lookups.USER_STATUS:
        status = UserStatus(user_status_id=s.id, name=s.name, description=s.desc)
        sess.merge(status)

    # insert user permission types
    for t in lookups.PERMISSION_TYPE:
        type = PermissionType(permission_type_id=t.id, name=t.name, description=t.desc)
        sess.merge(type)


if __name__ == '__main__':
    setupUserDB()
