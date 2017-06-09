from datetime import datetime

from tests.integration.baseTestAPI import BaseTestAPI
from dataactbroker.app import create_app
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.userModel import User
from dataactcore.models.jobModels import Submission
from dataactcore.models.stagingModels import DetachedAwardFinancialAssistance
from dataactcore.models.lookups import PUBLISH_STATUS_DICT
from dataactcore.models.domainModels import SubTierAgency


class DetachedUploadTests(BaseTestAPI):
    """ Test detached file upload """

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources (test data)"""
        super(DetachedUploadTests, cls).setUpClass()
        # TODO: refactor into a pytest fixture

        with create_app().app_context():
            # get the submission test user
            sess = GlobalDB.db().session
            cls.session = sess
            submission_user = sess.query(User).filter(User.email == cls.test_users['admin_user']).one()
            cls.submission_user_id = submission_user.user_id

            # setup submission/jobs data for test_check_status
            cls.d2_submission = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                      start_date="10/2015", end_date="12/2015", is_quarter=True)

            cls.d2_submission_dupe = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                           start_date="10/2015", end_date="12/2015", is_quarter=True)

            cls.d2_submission_dupe_2 = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                             start_date="10/2015", end_date="12/2015", is_quarter=True)

            cls.published_submission = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                             start_date="10/2015", end_date="12/2015", is_quarter=True,
                                                             publish_status_id=PUBLISH_STATUS_DICT["published"])

            cls.other_submission = cls.insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                         start_date="07/2015", end_date="09/2015",
                                                         is_quarter=True, d2_submission=False)

    def setUp(self):
        """Test set-up."""
        super(DetachedUploadTests, self).setUp()
        self.login_admin_user()

    def test_successful_submit_detached(self):
        submission = {"submission_id": self.d2_submission}
        response = self.app.post_json("/v1/submit_detached_file/", submission,
                                      headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 200)

    def test_already_published(self):
        submission = {"submission_id": self.published_submission}
        response = self.app.post_json("/v1/submit_detached_file/", submission,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual("Submission has already been published", response.json["message"])

    def test_not_fabs(self):
        submission = {"submission_id": self.other_submission}
        response = self.app.post_json("/v1/submit_detached_file/", submission,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual("Submission is not a FABS submission", response.json["message"])

    def test_duplicate_entry(self):
        self.insert_duplicate_detached_award()
        submission = {"submission_id": self.d2_submission_dupe}
        response = self.app.post_json("/v1/submit_detached_file/", submission,
                                      headers={"x-session-id": self.session_id})

        submission = {"submission_id": self.d2_submission_dupe_2}
        response = self.app.post_json("/v1/submit_detached_file/", submission,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 500)

    @staticmethod
    def insert_submission(sess, submission_user_id, cgac_code=None, start_date=None, end_date=None,
                          is_quarter=False, publish_status_id=1, d2_submission=True):
        """Insert one submission into job tracker and get submission ID back."""
        sub = Submission(created_at=datetime.utcnow(),
                         user_id=submission_user_id,
                         cgac_code=cgac_code,
                         reporting_start_date=datetime.strptime(start_date, '%m/%Y'),
                         reporting_end_date=datetime.strptime(end_date, '%m/%Y'),
                         is_quarter_format=is_quarter,
                         publish_status_id=publish_status_id,
                         d2_submission=d2_submission)
        sess.add(sub)
        sess.commit()
        return sub.submission_id

    def insert_duplicate_detached_award(self):

        sub_tier_agency = SubTierAgency(created_at=datetime.utcnow(), cgac_id=1,
                                        sub_tier_agency_code="abc", sub_tier_agency_name="test name")

        det_award = DetachedAwardFinancialAssistance(created_at=datetime.utcnow(),
                                                     submission_id=self.d2_submission_dupe,
                                                     job_id=1, row_number=1, is_valid=True,
                                                     fain="abc", uri="def", awarding_sub_tier_agency_c="abc",
                                                     award_modification_amendme="def")

        det_award_2 = DetachedAwardFinancialAssistance(created_at=datetime.utcnow(),
                                                       submission_id=self.d2_submission_dupe_2,
                                                       job_id=1, row_number=1, is_valid=True,
                                                       fain="abc", uri="def", awarding_sub_tier_agency_c="abc",
                                                       award_modification_amendme="def")

        self.session.add_all([det_award, det_award_2, sub_tier_agency])
        self.session.commit()
