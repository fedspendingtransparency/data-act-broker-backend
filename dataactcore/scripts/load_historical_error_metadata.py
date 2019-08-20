import logging

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.errorModels import ErrorMetadata, CertifiedErrorMetadata
from dataactcore.models.jobModels import Job, Submission
from dataactcore.models.lookups import PUBLISH_STATUS_DICT

from dataactvalidator.health_check import create_app

from dataactcore.models.validationModels import RuleSeverity # noqa
from dataactcore.models.userModel import User  # noqa

logger = logging.getLogger(__name__)


def move_certified_error_metadata(sess):
    """ Simply move the error metadata for certified submissions since that one is valid.

        Args:
            sess: connection to database 
    """
    logger.info("Moving certified error metadata")
    # Get a list of all certified jobs that aren't FABS
    certified_job_list = sess.query(Job.job_id).join(Submission, Job.submission_id == Submission.submission_id).\
        filter(Submission.d2_submission.is_(False), Submission.publish_status_id == PUBLISH_STATUS_DICT['published']).\
        all()

    # Delete all current certified entries to prevent duplicates
    sess.query(CertifiedErrorMetadata).filter(CertifiedErrorMetadata.job_id.in_(certified_job_list)).\
        delete(synchronize_session=False)

    # Create dict of error metadata
    error_metadata_objects = sess.query(ErrorMetadata).filter(ErrorMetadata.job_id.in_(certified_job_list)).all()
    error_metadata_list = []
    for obj in error_metadata_objects:
        tmp_obj = obj.__dict__
        tmp_obj.pop('_sa_instance_state')
        tmp_obj.pop('created_at')
        tmp_obj.pop('updated_at')
        tmp_obj.pop('error_metadata_id')
        error_metadata_list.append(obj.__dict__)

    # Save all the objects in the certified error metadata table
    sess.bulk_save_objects([CertifiedErrorMetadata(**error_metadata) for error_metadata in error_metadata_list])
    sess.commit()
    logger.info("Certified error metadata moved")


if __name__ == '__main__':
    db_sess = GlobalDB.db().session

    configure_logging()

    with create_app().app_context():
        move_certified_error_metadata(db_sess)
