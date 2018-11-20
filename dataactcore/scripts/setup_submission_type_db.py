from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models import lookups
from dataactcore.models.jobModels import ApplicationType
from dataactvalidator.health_check import create_app


def setup_submission_type_db():
    """Create job tracker tables from model metadata."""
    with create_app().app_context():
        sess = GlobalDB.db().session
        insert_codes(sess)
        sess.commit()


def insert_codes(sess):
    """Create job tracker tables from model metadata."""

    # insert application types
    for s in lookups.SUBMISSION_TYPE:
        submission_type = ApplicationType(application_type_id=s.id, application_name=s.name)
        sess.merge(submission_type)


if __name__ == '__main__':
    configure_logging()
    setup_submission_type_db()
