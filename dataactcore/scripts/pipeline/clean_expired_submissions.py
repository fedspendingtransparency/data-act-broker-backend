import logging
import argparse
import json
from dateutil.relativedelta import relativedelta

from dataactbroker.handlers.submission_handler import delete_all_submission_data

from dataactcore.broker_logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import get_utc_now
from dataactcore.models.jobModels import Submission
from dataactcore.models.lookups import PUBLISH_STATUS_DICT
from dataactcore.models.views import SubmissionUpdatedView

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def clean_expired_submissions(fy17q1_subs=False):
    """Cleans the database of expired submissions

    Definition of an expired submission:
        * unpublished
        * DABS test submission or certifiable FY17Q1 submissions
        * has not been updated (including any of its jobs) in over 6 months

    Args:
        fy17q1_subs: whether to specifically remove expired submissions from FY17Q1
    """

    sess = GlobalDB.db().session

    logger.info("Getting expired submissions")
    if fy17q1_subs:
        expired_submissions = (
            sess.query(Submission)
            .filter(
                Submission.publish_status_id == PUBLISH_STATUS_DICT["unpublished"],
                Submission.is_fabs.is_(False),
                Submission.reporting_fiscal_year == 2017,
                Submission.reporting_fiscal_period == 3,
            )
            .all()
        )
    else:
        updated_at_view = SubmissionUpdatedView()
        expiration_cutoff = get_utc_now() - relativedelta(months=6)
        expired_submissions = (
            sess.query(Submission)
            .filter(
                Submission.publish_status_id == PUBLISH_STATUS_DICT["unpublished"],
                Submission.test_submission.is_(True),
                updated_at_view.updated_at < expiration_cutoff,
            )
            .outerjoin(updated_at_view.table, updated_at_view.submission_id == Submission.submission_id)
            .all()
        )
    expired_submission_ids = [exp_sub.submission_id for exp_sub in expired_submissions]
    logger.info("Expired submissions (count: {}): {}".format(len(expired_submission_ids), expired_submission_ids))

    logger.info("Deleting expired submissions")
    for submission in expired_submissions:
        delete_all_submission_data(submission)
        sess.commit()
    logger.info("Database cleaned of expired submissions")

    return expired_submission_ids


if __name__ == "__main__":
    with create_app().app_context():
        configure_logging()

        parser = argparse.ArgumentParser(description="Clean expired submissions")
        parser.add_argument(
            "-fy17q1", "--fy17q1", help="Specifically remove expired submissions from FY17Q1", action="store_true"
        )
        args = parser.parse_args()

        start_time = get_utc_now()
        metrics = {
            "script_name": "clean_expired_submissions.py",
            "start_time": str(start_time),
        }

        expired_subs = clean_expired_submissions(args.fy17q1)

        metrics["subs_removed"] = expired_subs
        metrics["subs_removed_count"] = len(expired_subs)
        metrics["duration"] = str(get_utc_now() - start_time)
        with open("clean_expired_subs_metrics.json", "w+") as metrics_file:
            json.dump(metrics, metrics_file)
