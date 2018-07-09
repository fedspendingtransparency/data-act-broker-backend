from datetime import datetime

from tests.integration.baseTestAPI import BaseTestAPI
from dataactbroker.app import create_app
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.userModel import User
from dataactcore.models.jobModels import Submission
from dataactcore.models.lookups import PUBLISH_STATUS_DICT


class DetachedUploadTests(BaseTestAPI):
    """ Test detached file upload """

    @classmethod
    def setUpClass(cls):
        """ Set up class-wide resources (test data) """
        super(DetachedUploadTests, cls).setUpClass()
        # TODO: refactor into a pytest fixture

        with create_app().app_context():
            # get the submission test users
            sess = GlobalDB.db().session
            cls.session = sess
            admin_user = sess.query(User).filter(User.email == cls.test_users['admin_user']).one()
            agency_user = sess.query(User).filter(User.email == cls.test_users['agency_user']).one()
            cls.admin_user_id = admin_user.user_id
            cls.agency_user_id = agency_user.user_id

            # setup submission/jobs data for test_check_status
            cls.d2_submission = cls.insert_submission(sess, cls.admin_user_id, cgac_code="SYS",
                                                      start_date="10/2015", end_date="12/2015", is_quarter=True)

            cls.d2_submission_2 = cls.insert_submission(sess, cls.agency_user_id, cgac_code="SYS",
                                                        start_date="10/2015", end_date="12/2015", is_quarter=True)

            cls.published_submission = cls.insert_submission(sess, cls.admin_user_id, cgac_code="SYS",
                                                             start_date="10/2015", end_date="12/2015", is_quarter=True,
                                                             publish_status_id=PUBLISH_STATUS_DICT["published"])

            cls.other_submission = cls.insert_submission(sess, cls.admin_user_id, cgac_code="SYS",
                                                         start_date="07/2015", end_date="09/2015",
                                                         is_quarter=True, d2_submission=False)

    def setUp(self):
        """Test set-up."""
        super(DetachedUploadTests, self).setUp()
        self.login_admin_user()

    def test_successful_submit_detached(self):
        """ Test a successful publish """
        submission = {"submission_id": self.d2_submission}
        response = self.app.post_json("/v1/submit_detached_file/", submission,
                                      headers={"x-session-id": self.session_id})
        self.assertEqual(response.status_code, 200)

    def test_owner_no_permissions(self):
        """ Test a publish failure despite being the owner of the submission because no permissions """
        self.logout()
        self.login_user()
        submission = {"submission_id": self.d2_submission_2}
        response = self.app.post_json("/v1/submit_detached_file/", submission,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 403)
        self.assertEqual("User does not have permission to access that submission", response.json["message"])

    def test_already_published(self):
        """ Test a publish failure because the submission is already published """
        submission = {"submission_id": self.published_submission}
        response = self.app.post_json("/v1/submit_detached_file/", submission,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual("Submission has already been published", response.json["message"])

    def test_not_fabs(self):
        """ Test a publish failure because the submission is not FABS """
        submission = {"submission_id": self.other_submission}
        response = self.app.post_json("/v1/submit_detached_file/", submission,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual("Submission is not a FABS submission", response.json["message"])

    @staticmethod
    def insert_submission(sess, submission_user_id, cgac_code=None, start_date=None, end_date=None,
                          is_quarter=False, publish_status_id=1, d2_submission=True):
        """ Insert one submission into job tracker and get submission ID back.

            Args:
                sess: the current session
                submission_user_id: the ID of the user that owns the submission
                cgac_code: cgac code of the agency that the submission is to, default None
                start_date: the reporting start date of the submission, default None
                end_date: the reporting start date of the submission, default None
                is_quarter: boolean indicating if the submission is a quarterly (false is monthly), default False
                publish_status_id: the publish status of the submission, default 1 (unpublished)
                d2_submission: boolean indicating if the submission is FABS or DABS (true for FABS), default True

            Returns:
                the submission ID of the created submission
        """
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
