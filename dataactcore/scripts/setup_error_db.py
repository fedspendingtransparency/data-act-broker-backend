from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.errorModels import FileStatus, ErrorType
from dataactcore.models import lookups
from dataactvalidator.health_check import create_app


def setup_error_db():
    """Create error tables from model metadata."""

    with create_app().app_context():
        sess = GlobalDB.db().session
        insert_codes(sess)
        sess.commit()


def insert_codes(sess):
    """Insert static data."""

    # insert file status types
    for s in lookups.FILE_STATUS:
        status = FileStatus(file_status_id=s.id, name=s.name, description=s.desc)
        sess.merge(status)

    # insert error types
    for e in lookups.ERROR_TYPE:
        error = ErrorType(error_type_id=e.id, name=e.name, description=e.desc)
        sess.merge(error)


if __name__ == '__main__':
    configure_logging()
    setup_error_db()
