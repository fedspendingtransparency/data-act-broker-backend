from dataactcore.models.errorModels import FileStatus, ErrorType
from dataactcore.interfaces.db import databaseSession


def setupErrorDB():
    """Create error tables from model metadata."""
    with databaseSession() as sess:
        insertCodes(sess)
        sess.commit()

def insertCodes(sess):
    """Insert static data."""

    # insert file status types

    statusList = [(1, 'complete', 'File has been processed'),
        (2, 'header_error', 'The file has errors in the header row'),
        (3, 'unknown_error', 'An unknown error has occurred with this file'),
        (4, 'single_row_error', 'CSV file must have a header row and at least one record'),
        (5, 'job_error', 'Error occurred in job manager'),
        (6, 'incomplete', 'File has not yet been validated')]
    for s in statusList:
        status = FileStatus(file_status_id=s[0], name=s[1], description=s[2])
        sess.merge(status)

    # insert error types
    errorList = [(1, 'type_error', 'The value provided was of the wrong type'),
        (2, 'required_error', 'A required value was not provided'),
        (3, 'value_error', 'The value provided was invalid'),
        (4, 'read_error', 'Could not parse this record correctly'),
        (5, 'write_error', 'Could not write this record into the staging table'),
        (6, 'rule_failed', 'A rule failed for this value'),
        (7, 'length_error', 'Value was longer than allowed length')]
    for e in errorList:
        error = ErrorType(error_type_id=e[0], name=e[1], description=e[2])
        sess.merge(error)


if __name__ == '__main__':
    setupErrorDB()
