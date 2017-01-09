from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models import lookups
from dataactcore.models.userModel import PermissionType
from dataactvalidator.app import createApp


def setupUserDB():
    """Create user tables from model metadata."""
    with createApp().app_context():
        sess = GlobalDB.db().session
        insertCodes(sess)
        sess.commit()


def insertCodes(sess):
    """Create job tracker tables from model metadata."""
    # insert user permission types
    for t in lookups.PERMISSION_TYPE:
        permission_type = PermissionType(permission_type_id=t.id, name=t.name, description=t.desc)
        sess.merge(permission_type)


if __name__ == '__main__':
    configure_logging()
    setupUserDB()
