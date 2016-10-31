from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from dataactcore.models.jobTrackerInterface import obligationStatsForSubmission

def test_obligationStatsForSubmission_nonzero(database):
    submission = SubmissionFactory()
    database.session.add(submission)
    database.session.commit()
    financials = [
        AwardFinancialFactory(transaction_obligated_amou=1000, piid="1234", fain=None, uri=None, 
                              submission_id=submission.submission_id),
        AwardFinancialFactory(transaction_obligated_amou=1000, piid="1235", fain=None, uri=None, 
                              submission_id=submission.submission_id),
        AwardFinancialFactory(transaction_obligated_amou=1000, piid=None, fain="1235", uri=None, 
                              submission_id=submission.submission_id),
        AwardFinancialFactory(transaction_obligated_amou=1000, piid=None, fain=None, uri="1235", 
                              submission_id=submission.submission_id),
        AwardFinancialFactory(transaction_obligated_amou=1000, piid=None, fain="1234", uri="1235", 
                              submission_id=submission.submission_id),
    ]
    database.session.add_all(financials)
    database.session.commit()
    assert obligationStatsForSubmission(submission.submission_id) == {
        "total_obligations": 5000,
        "total_procurement_obligations": 2000,
        "total_assistance_obligations": 3000
    }

def test_obligationStatsForSubmission_zero(database):
    submission = SubmissionFactory()
    # no financials in db
    database.session.add(submission)
    database.session.commit()
    assert obligationStatsForSubmission(submission.submission_id) == {
        "total_obligations": 0,
        "total_procurement_obligations": 0,
        "total_assistance_obligations": 0
    }