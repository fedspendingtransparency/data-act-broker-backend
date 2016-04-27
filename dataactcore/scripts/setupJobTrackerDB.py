from dataactcore.models.jobModels import Status, Type, FileType
from dataactcore.models.jobTrackerInterface import JobTrackerInterface
from dataactcore.scripts.databaseSetup import createDatabase, runMigrations
from dataactcore.config import CONFIG_DB


def setupJobTrackerDB():
    """Create job tracker tables from model metadata."""
    createDatabase(CONFIG_DB['job_db_name'])
    runMigrations('job_tracker')
    insertCodes()


def insertCodes():
    """Create job tracker tables from model metadata."""
    jobDb = JobTrackerInterface()

    # TODO: define these codes as enums in the data model?
    # insert status types
    statusList = [(1, 'waiting', 'check dependency table'),
        (2, 'ready', 'can be assigned'),
        (3, 'running', 'job is currently in progress'),
        (4, 'finished', 'job is complete'),
        (5, 'invalid', 'job is invalid'),
        (6, 'failed', 'job failed to complete')]
    for s in statusList:
        status = Status(status_id=s[0], name=s[1], description=s[2])
        jobDb.session.merge(status)

    typeList = [(1, 'file_upload', 'file must be uploaded to S3'),
        (2, 'csv_record_validation', 'do record level validation and add to staging DB'),
        (3, 'db_transfer', 'information must be moved from production DB to staging DB'),
        (4, 'validation', 'new information must be validated'),
        (5, 'external_validation', 'new information must be validated against external sources')]
    for t in typeList:
        thisType = Type(type_id=t[0],name=t[1], description=t[2])
        jobDb.session.merge(thisType)

    fileTypeList = [(1, 'award', ''),
        (2, 'award_financial', ''),
        (3, 'appropriations', ''),
        (4, 'program_activity', '')]
    for ft in fileTypeList:
        fileType = FileType(file_type_id=ft[0], name=ft[1], description=ft[2])
        jobDb.session.merge(fileType)

    jobDb.session.commit()
    jobDb.session.close()

if __name__ == '__main__':
    setupJobTrackerDB()
