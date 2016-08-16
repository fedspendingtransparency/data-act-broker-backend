from dataactcore.models.jobModels import JobStatus, JobType, FileType, PublishStatus
from dataactcore.models.jobTrackerInterface import JobTrackerInterface
from dataactcore.scripts.databaseSetup import createDatabase, runMigrations
from dataactcore.config import CONFIG_DB


def setupJobTrackerDB():
    """Create job tracker tables from model metadata."""
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
        status = JobStatus(job_status_id=s[0], name=s[1], description=s[2])
        jobDb.session.merge(status)

    typeList = [(1, 'file_upload', 'file must be uploaded to S3'),
        (2, 'csv_record_validation', 'do record level validation and add to staging table'),
        (3, 'db_transfer', 'information must be moved from production DB to staging table'),
        (4, 'validation', 'new information must be validated'),
        (5, 'external_validation', 'new information must be validated against external sources')]
    for t in typeList:
        thisType = JobType(job_type_id=t[0],name=t[1], description=t[2])
        jobDb.session.merge(thisType)

    publishStatusList = [(1, 'unpublished', 'Has not yet been moved to data store'),
        (2,'published', 'Has been moved to data store'),
        (3, 'updated', 'Submission was updated after being published')]
    for ps in publishStatusList:
        status = PublishStatus(publish_status_id=ps[0], name=ps[1], description=ps[2])
        jobDb.session.merge(status)

    fileTypeList = [(1, 'appropriations', ''),
        (2,'program_activity', ''),
        (3, 'award_financial', ''),
        (4, 'award', ''),
        (5, 'award_procurement', '')]
    for ft in fileTypeList:
        fileType = FileType(file_type_id=ft[0], name=ft[1], description=ft[2])
        jobDb.session.merge(fileType)

    jobDb.session.commit()
    jobDb.session.close()

if __name__ == '__main__':
    setupJobTrackerDB()
