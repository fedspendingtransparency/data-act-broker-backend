from dataactbroker.app import createApp
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models import lookups
from dataactcore.models.errorModels import FileStatus, ErrorType


def setupErrorDB():
    """Create error tables from model metadata."""

    with createApp().app_context():
        sess = GlobalDB.db().session
        insertCodes(sess)
        sess.commit()


def insertCodes(sess):
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
    setupErrorDB()
