from dataactbroker.handlers.submission_handler import get_submission_stats

from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import TotalObligationsFactory


def test_obligation_stats_for_submission_nonzero(database):
    submission = SubmissionFactory()
    database.session.add(submission)
    database.session.commit()
    financials = [
        TotalObligationsFactory(
            total_obligations=5000,
            total_proc_obligations=2000,
            total_asst_obligations=3000,
            submission_id=submission.submission_id,
        )
    ]
    database.session.add_all(financials)
    database.session.commit()
    assert get_submission_stats(submission.submission_id) == {
        "total_obligations": 5000,
        "total_procurement_obligations": 2000,
        "total_assistance_obligations": 3000,
    }


def test_obligation_stats_for_submission_zero(database):
    submission = SubmissionFactory()
    # no financials in db
    database.session.add(submission)
    database.session.commit()
    assert get_submission_stats(submission.submission_id) == {
        "total_obligations": 0,
        "total_procurement_obligations": 0,
        "total_assistance_obligations": 0,
    }
