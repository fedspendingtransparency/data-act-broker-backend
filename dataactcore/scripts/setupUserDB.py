from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models import lookups
from dataactcore.models.userModel import PermissionType
from dataactvalidator.health_check import create_app


def setup_user_db():
    """Create user tables from model metadata."""
    with create_app().app_context():
        sess = GlobalDB.db().session
        insert_codes(sess)
        sess.commit()


def insert_codes(sess):
    """Create job tracker tables from model metadata."""
    # insert user permission types
    for t in lookups.PERMISSION_TYPES:
        permission_type = PermissionType(permission_type_id=t.id, name=t.name, description=t.desc)
        sess.merge(permission_type)


if __name__ == '__main__':
    configure_logging()
    setup_user_db()
