from dataactcore.models import errorModels
from dataactcore.models.errorModels import Status, ErrorType
from dataactcore.models.errorInterface import ErrorInterface
from dataactcore.scripts.databaseSetup import createDatabase
from dataactcore.config import CONFIG_DB


def setupErrorDB(hardReset = False):
    """Create job tracker tables from model metadata."""
    createDatabase(CONFIG_DB['error_db_name'])
    errorDb = ErrorInterface()
    if hardReset:
        errorModels.Base.metadata.drop_all(errorDb.engine)
    errorModels.Base.metadata.create_all(errorDb.engine)

    # TODO: define these codes as enums in the data model?

    # insert status types
    statusList = [(1, 'complete', 'File has been processed'),
        (2, 'missing_header_error', 'One of the required columns is not present in the file'),
        (3, 'bad_header_error', 'One of the headers in the file is not recognized'),
        (4, 'unknown_error', 'An unknown error has occurred with this file'),
        (5,'single_row_error','CSV file must have a header row and at least one record'),
        (6,'duplicate_header_error','May not have the same header twice'),
        (7,'job_error','Error occurred in job manager')]
    for s in statusList:
        status = Status(status_id=s[0], name=s[1], description=s[2])
        errorDb.session.add(status)

    # insert error types
    errorList = [(1, 'type_error', 'The value provided was of the wrong type'),
        (2, 'required_error', 'A required value was not provided'),
        (3, 'value_error', 'The value provided was invalid'),
        (4, 'read_error', 'Could not parse this record correctly'),
        (5, 'write_error', 'Could not write this record into the staging database')]
    for e in errorList:
        error = ErrorType(error_type_id=e[0], name=e[1], description=e[2])
        errorDb.session.add(error)

    errorDb.session.commit()
    errorDb.session.close()

if __name__ == '__main__':
    setupErrorDB(True)
