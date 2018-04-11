from sqlalchemy import or_

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models import lookups
from dataactcore.models.jobModels import JobStatus, JobType, FileType, PublishStatus
from dataactvalidator.health_check import create_app


def setup_job_tracker_db():
    """Create job tracker tables from model metadata."""
    with create_app().app_context():
        sess = GlobalDB.db().session
        insert_codes(sess)
        delete_unused_job_types(sess)
        sess.commit()


def insert_codes(sess):
    """Create job tracker tables from model metadata."""

    # TODO: define these codes as enums in the data model?
    # insert status types
    for s in lookups.JOB_STATUS:
        status = JobStatus(job_status_id=s.id, name=s.name, description=s.desc)
        sess.merge(status)

    # insert job types
    for t in lookups.JOB_TYPE:
        this_type = JobType(job_type_id=t.id, name=t.name, description=t.desc)
        sess.merge(this_type)

    # insert publish status
    for ps in lookups.PUBLISH_STATUS:
        status = PublishStatus(publish_status_id=ps.id, name=ps.name, description=ps.desc)
        sess.merge(status)

    # insert file types
    for ft in lookups.FILE_TYPE:
        file_type = FileType(
            file_type_id=ft.id,
            name=ft.name,
            description=ft.desc,
            letter_name=ft.letter,
            file_order=ft.order
        )
        sess.merge(file_type)


def delete_unused_job_types(sess):
    """
    Deletes the job_type and jobs associated with db_transfer and external_validation that are not used
    """

    # Using raw sql to delete jobs to avoid sqlalchemy cascading that deletes the entire submission

    # Delete related jobs from job_dependency table
    delete_job_dependency_statement = """
     DELETE FROM job_dependency
     USING job, job_type
     WHERE job_dependency.job_id = job.job_id
        AND job.job_type_id = job_type.job_type_id
        AND (job_type.name = 'db_transfer' OR job_type.name = 'external_validation');
     """

    # Delete related jobs from job table
    delete_job_statement = """
    DELETE FROM job
    USING job_type
    WHERE job.job_type_id = job_type.job_type_id
        AND (job_type.name = 'db_transfer' OR job_type.name = 'external_validation');
    """

    sess.execute(delete_job_dependency_statement)
    sess.execute(delete_job_statement)

    # Delete unused job types from job_type table
    job_type_query = sess.query(JobType).filter(or_(JobType.name == 'db_transfer',
                                                    JobType.name == 'external_validation'))

    for job_type in job_type_query:
        sess.delete(job_type)


if __name__ == '__main__':
    configure_logging()
    setup_job_tracker_db()
