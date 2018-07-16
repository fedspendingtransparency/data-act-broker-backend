from datetime import datetime

from tests.integration.baseTestAPI import BaseTestAPI
from dataactbroker.app import create_app
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import CGAC, FREC, SubTierAgency
from dataactcore.models.userModel import User
from dataactcore.models.jobModels import Submission, Job
from dataactcore.models.lookups import PUBLISH_STATUS_DICT, FILE_STATUS_DICT, FILE_TYPE_DICT, JOB_TYPE_DICT


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
            cls.agency_user_email = agency_user.email

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

            cls.test_agency_user_submission_id = cls.insert_submission(sess, cls.agency_user_id, cgac_code="NOT",
                                                                       start_date="10/2015", end_date="12/2015",
                                                                       is_quarter=True, d2_submission=True)
            cls.insert_agency_user_submission_data(sess, cls.test_agency_user_submission_id)

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
        self.assertEqual(response.json["message"], "User does not have permission to access that submission")

    def test_already_published(self):
        """ Test a publish failure because the submission is already published """
        submission = {"submission_id": self.published_submission}
        response = self.app.post_json("/v1/submit_detached_file/", submission,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json["message"], "Submission has already been published")

    def test_not_fabs(self):
        """ Test a publish failure because the submission is not FABS """
        submission = {"submission_id": self.other_submission}
        response = self.app.post_json("/v1/submit_detached_file/", submission,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json["message"], "Submission is not a FABS submission")

    def test_upload_detached_file_wrong_permissions_wrong_user(self):
        self.login_user()
        new_submission_json = {
            "agency_code": "WRONG",
            "is_quarter": True,
            "reporting_period_start_date": "07/2015",
            "reporting_period_end_date": "09/2015"}
        response = self.app.post_json("/v1/upload_detached_file/", new_submission_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json['message'], "User does not have permissions to write to that subtier agency")

    def test_upload_detached_file_wrong_permissions_right_user(self):
        self.login_user(username=self.agency_user_email)
        update_submission_json = {
            "existing_submission_id": str(self.test_agency_user_submission_id),
            "fabs": "fabs.csv",
            "is_quarter": True,
            "reporting_period_start_date": "10/2015",
            "reporting_period_end_date": "12/2015"}
        response = self.app.post_json("/v1/upload_detached_file/", update_submission_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 200)

    def test_upload_detached_file_missing_parameters(self):
        self.login_user(username=self.agency_user_email)
        update_submission_json = {
            "is_quarter": True,
            "reporting_period_start_date": "10/2015",
            "reporting_period_end_date": "12/2015"}
        response = self.app.post_json("/v1/upload_detached_file/", update_submission_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Missing required parameter: agency_code or existing_submission_id')

    def test_upload_detached_file_incorrect_parameters(self):
        self.login_user(username=self.agency_user_email)
        update_submission_json = {
            "existing_submission_id": "-99",
            "is_quarter": True,
            "reporting_period_start_date": "10/2015",
            "reporting_period_end_date": "12/2015"}
        response = self.app.post_json("/v1/upload_detached_file/", update_submission_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'existing_submission_id must be a valid submission_id')

    def test_upload_detached_file_missing_fabs(self):
        new_submission_json = {
            "agency_code": "WRONG",
            "is_quarter": False}
        response = self.app.post_json("/v1/upload_detached_file/", new_submission_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], "fabs: Missing data for required field.")

    def test_upload_detached_file_dabs_submission(self):
        new_submission_json = {
            "existing_submission_id": str(self.other_submission),
            "is_quarter": False,
            "fabs": "test_file.csv"}
        response = self.app.post_json("/v1/upload_detached_file/", new_submission_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], "Existing submission must be a FABS submission")

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

    @staticmethod
    def insert_agency_user_submission_data(sess, submission_id):
        """Insert jobs for the submission, and create a CGAC, FREC, and SubTierAgency"""
        for job_type in ['file_upload', 'csv_record_validation', 'validation']:
            sess.add(Job(file_type_id=FILE_TYPE_DICT['fabs'], job_status_id=FILE_STATUS_DICT['complete'],
                         job_type_id=JOB_TYPE_DICT[job_type], submission_id=submission_id, original_filename=None,
                         file_size=None, number_of_rows=None))
            sess.commit()

        cgac = CGAC(cgac_code="NOT")
        sess.add(cgac)
        sess.commit()
        frec = FREC(cgac_id=cgac.cgac_id, frec_code="BLAH")
        sess.add(frec)
        sess.commit()
        sub = SubTierAgency(sub_tier_agency_code="WRONG", cgac_id=cgac.cgac_id, frec_id=frec.frec_id, is_frec=False)
        sess.add(sub)
        sess.commit()
