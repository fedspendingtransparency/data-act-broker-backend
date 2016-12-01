from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models import lookups
from dataactcore.models.jobModels import JobStatus, JobType, FileType, PublishStatus
from dataactvalidator.app import createApp


def setupJobTrackerDB():
    """Create job tracker tables from model metadata."""
    with createApp().app_context():
        sess = GlobalDB.db().session
        insertCodes(sess)
        sess.commit()


def insertCodes(sess):
    """Create job tracker tables from model metadata."""

    # TODO: define these codes as enums in the data model?
    # insert status types
    for s in lookups.JOB_STATUS:
        status = JobStatus(job_status_id=s.id, name=s.name, description=s.desc)
        sess.merge(status)

    # insert job types
    for t in lookups.JOB_TYPE:
        thisType = JobType(job_type_id=t.id, name=t.name, description=t.desc)
        sess.merge(thisType)

    # insert publish status
    for ps in lookups.PUBLISH_STATUS:
        status = PublishStatus(publish_status_id=ps.id, name=ps.name, description=ps.desc)
        sess.merge(status)

    # insert file types
    for ft in lookups.FILE_TYPE:
        fileType = FileType(
            file_type_id=ft.id,
            name=ft.name,
            description=ft.desc,
            letter_name=ft.letter,
            file_order=ft.order
        )
        sess.merge(fileType)


if __name__ == '__main__':
    configure_logging()
    setupJobTrackerDB()
