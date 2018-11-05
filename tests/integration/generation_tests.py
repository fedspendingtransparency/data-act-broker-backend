from dataactcore.interfaces.db import GlobalDB

from dataactcore.models.jobModels import Submission, JobDependency
from dataactcore.models.userModel import User
from dataactcore.models.lookups import FILE_TYPE_DICT, JOB_TYPE_DICT, JOB_STATUS_DICT

from dataactvalidator.health_check import create_app

from tests.integration.baseTestAPI import BaseTestAPI
from tests.integration.integration_test_helper import insert_submission, insert_job


class GenerationTests(BaseTestAPI):
    """Test file generation routes."""

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources (test data)"""
        super(GenerationTests, cls).setUpClass()
        # TODO: refactor into a pytest fixture

        with create_app().app_context():
            # get the submission test user
            sess = GlobalDB.db().session
            submission_user = sess.query(User).filter(User.email == cls.test_users['admin_user']).one()
            cls.submission_user_id = submission_user.user_id

            other_user = sess.query(User).filter(User.email == cls.test_users['agency_user']).one()
            cls.other_user_email = other_user.email
            cls.other_user_id = other_user.user_id

            # setup submission/jobs data for test_check_status
            cls.generation_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                             start_date="07/2015", end_date="09/2015", is_quarter=True)
            cls.setup_file_generation_submission(sess)

            cls.test_fabs_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code="SYS",
                                                            start_date="10/2015", end_date="12/2015", is_quarter=False,
                                                            number_of_errors=0, is_fabs=True)

    def setUp(self):
        """Test set-up."""
        super(GenerationTests, self).setUp()
        self.login_admin_user()

    def test_bad_file_type_check_generation_status(self):
        """ Test that an error comes back if an invalid file type is provided for check_generation_status. """
        post_json = {"submission_id": self.generation_submission_id, "file_type": "A"}
        response = self.app.get("/v1/check_generation_status/", post_json, headers={"x-session-id": self.session_id},
                                expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json["message"], "file_type: Must be either D1, D2, E or F")

    def test_check_generation_status_finished(self):
        """ Test the check generation status route for finished generation """
        # Then call check generation route for D2, E and F and check results
        post_json = {"submission_id": self.generation_submission_id, "file_type": "E"}
        response = self.app.get("/v1/check_generation_status/", post_json, headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)
        json = response.json
        self.assertEqual(json["status"], "finished")
        self.assertEqual(json["file_type"], "E")
        self.assertEqual(json["url"], "#")
        self.assertEqual(json["message"], "")

    def test_check_generation_status_failed_file_level_errors(self):
        """ Test the check generation status route for a failed generation because of file level errors """
        post_json = {"submission_id": self.generation_submission_id, "file_type": "D2"}
        response = self.app.get("/v1/check_generation_status/", post_json, headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)
        json = response.json
        self.assertEqual(json["status"], "failed")
        self.assertEqual(json["file_type"], "D2")
        self.assertEqual(json["url"], "#")
        self.assertEqual(json["message"], "Generated file had file-level errors")

    def test_check_generation_status_failed_invalid_file(self):
        """ Test the check generation status route for a failed generation because of an invalid file """
        post_json = {"submission_id": self.generation_submission_id, "file_type": "F"}
        response = self.app.get("/v1/check_generation_status/", post_json, headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)
        json = response.json
        self.assertEqual(json["status"], "failed")
        self.assertEqual(json["file_type"], "F")
        self.assertEqual(json["url"], "#")
        self.assertEqual(json["message"], "File was invalid")

    def test_file_generation_d1(self):
        """ Test the generate route for D1 file """
        # For file generation submission, call generate route for D1 and check results
        post_json = {"submission_id": self.generation_submission_id, "file_type": "D1",
                     "start": "01/02/2016", "end": "02/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json, headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)
        json = response.json

        # use_aws is true when the PR unit tests run so the date range specified returns no results.
        # checking is in place for "failed" until use_aws is flipped to false
        self.assertIn(json["status"], ["failed", "waiting", "finished"])
        self.assertEqual(json["file_type"], "D1")
        self.assertIn("url", json)
        self.assertEqual(json["start"], "01/02/2016")
        self.assertEqual(json["end"], "02/03/2016")

        # this is to accommodate for checking for the "failed" status
        self.assertIn(json["message"], ["", "D1 data unavailable for the specified date range"])

    def test_generate_file_invalid_file_type(self):
        """ Test invalid file type passed to generate file """
        post_json = {"submission_id": self.generation_submission_id, "file_type": "A",
                     "start": "01/02/2016", "end": "02/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)

        self.assertEqual(response.status_code, 400)
        json = response.json
        self.assertEqual(json["message"], "file_type: Must be either D1, D2, E or F")

    def test_generate_file_bad_start_date_format(self):
        """ Test bad format on start date """
        post_json = {"submission_id": self.generation_submission_id, "file_type": "D1",
                     "start": "ab/02/2016", "end": "02/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)

        self.assertEqual(response.status_code, 400)
        json = response.json
        self.assertEqual(json["message"], "start: Must be in the format MM/DD/YYYY")

    def test_generate_file_bad_end_date_format(self):
        """ Test bad format on start date """
        post_json = {"submission_id": self.generation_submission_id, "file_type": "D1",
                     "start": "01/02/2016", "end": "ab/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)

        self.assertEqual(response.status_code, 400)
        json = response.json
        self.assertEqual(json["message"], "end: Must be in the format MM/DD/YYYY")

    def test_generate_d_file_no_start(self):
        """ Test that there is an error if no start date is provided for D file generation. """
        post_json = {"submission_id": self.generation_submission_id, "file_type": "D1", "end": "02/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)

        self.assertEqual(response.status_code, 400)
        json = response.json
        self.assertEqual(json["message"], "Must have a start and end date for D file generation.")

    def test_generate_ef_file_no_start(self):
        """ Test that there is no error when no start date is provided for E/F file generation """
        post_json = {"submission_id": self.generation_submission_id, "file_type": "E", "end": "02/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json, headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)

    def test_generate_file_fabs(self):
        """ Test failure while calling generate_file for a FABS submission """
        post_json = {"submission_id": self.test_fabs_submission_id, "file_type": "D1",
                     "start": "01/02/2016", "end": "02/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json, headers={"x-session-id": self.session_id},
                                      expect_errors=True)

        self.assertEqual(response.status_code, 400)
        json = response.json
        self.assertEqual(json["message"], "Cannot generate files for FABS submissions.")

    def test_generate_file_permission_error(self):
        """ Test permission error for generate submission """
        self.login_user()
        post_json = {"submission_id": self.generation_submission_id, "file_type": "D1",
                     "start": "01/02/2016", "end": "02/03/2016"}
        response = self.app.post_json("/v1/generate_file/", post_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)

        self.assertEqual(response.status_code, 403)
        json = response.json
        self.assertEqual(json["message"], "User does not have permission to access that submission")

    def test_detached_d_file_generation(self):
        """ Test the generate and check routes for external D files """
        # For file generation submission, call generate route for D1 and check results
        post_json = {'file_type': 'D1', 'start': '01/02/2016', 'end': '02/03/2016', 'cgac_code': '020'}
        response = self.app.post_json("/v1/generate_detached_file/", post_json,
                                      headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)
        json = response.json
        self.assertIn(json["status"], ["waiting", "running", "finished"])
        self.assertEqual(json["file_type"], "D1")
        self.assertIn("url", json)
        self.assertEqual(json["start"], "01/02/2016")
        self.assertEqual(json["end"], "02/03/2016")
        self.assertEqual(json["message"], "")
        self.assertIsNotNone(json["job_id"])

        # call check generation status route for D2 and check results
        post_json = {}
        response = self.app.get("/v1/check_detached_generation_status/", post_json,
                                headers={"x-session-id": self.session_id}, expect_errors=True)
        assert response.json['message'] == 'job_id: Missing data for required field.'

        post_json = {'job_id': -1}
        response = self.app.get("/v1/check_detached_generation_status/", post_json,
                                headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.json["message"], 'No generation job found with the specified ID')

    def test_detached_d_file_generation_quarter_fail(self):
        """ Test that detached D file generation fails if only the quarter is provided """
        post_json = {'file_type': 'D1', 'quarter': 'Q1/2018', 'cgac_code': '020'}
        response = self.app.post_json("/v1/generate_detached_file/", post_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json["message"], "Must have a start and end date for D file generation.")

    def test_detached_a_file_generation(self):
        """ Test the generate and check routes for external A files """
        post_json = {'file_type': 'A', 'quarter': 'Q1/2018', 'cgac_code': '020'}
        response = self.app.post_json("/v1/generate_detached_file/", post_json,
                                      headers={"x-session-id": self.session_id})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["message"], "This functionality is in development and coming soon.")

    def test_detached_a_file_generation_quarter_format_fail(self):
        """ Test that detached A file generation fails for bad quarter format. """
        # Wrong number
        post_json = {'file_type': 'A', 'quarter': 'Q5/2345', 'cgac_code': '020'}
        response = self.app.post_json("/v1/generate_detached_file/", post_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json["message"], "Quarter must be in Q#/YYYY format, where # is 1-4.")

        # Too many numbers
        post_json['quarter'] = 'Q11/2345'
        response = self.app.post_json("/v1/generate_detached_file/", post_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json["message"], "Quarter must be in Q#/YYYY format, where # is 1-4.")

        # Invalid year
        post_json['quarter'] = 'Q1/123'
        response = self.app.post_json("/v1/generate_detached_file/", post_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json["message"], "Quarter must be in Q#/YYYY format, where # is 1-4.")

    def test_detached_a_file_generation_start_end_fail(self):
        """ Test that detached A file generation fails if no quarter is provided """
        post_json = {'file_type': 'A', 'start': '01/02/2016', 'end': '02/03/2016', 'cgac_code': '020'}
        response = self.app.post_json("/v1/generate_detached_file/", post_json,
                                      headers={"x-session-id": self.session_id}, expect_errors=True)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json["message"], "Must have a quarter for A file generation.")

    @classmethod
    def setup_file_generation_submission(cls, sess, submission_id=None):
        """Create jobs for D, E, and F files."""
        submission_id = cls.generation_submission_id if not submission_id else submission_id
        submission = sess.query(Submission).filter(Submission.submission_id == submission_id).one()

        # Create D1 jobs ready for generation route to be called
        insert_job(
            sess,
            FILE_TYPE_DICT['award_procurement'],
            JOB_STATUS_DICT['ready'],
            JOB_TYPE_DICT['file_upload'],
            submission.submission_id
        )
        award_roc_val_job = insert_job(
            sess,
            FILE_TYPE_DICT['award_procurement'],
            JOB_STATUS_DICT['waiting'],
            JOB_TYPE_DICT['csv_record_validation'],
            submission.submission_id
        )
        # Create E and F jobs ready for check route
        exec_comp_job = insert_job(
            sess,
            FILE_TYPE_DICT['executive_compensation'],
            JOB_STATUS_DICT['finished'],
            JOB_TYPE_DICT['file_upload'],
            submission.submission_id
        )
        sub_award_job = insert_job(
            sess,
            FILE_TYPE_DICT['sub_award'],
            JOB_STATUS_DICT['invalid'],
            JOB_TYPE_DICT['file_upload'],
            submission.submission_id
        )
        sub_award_job.error_message = "File was invalid"

        # Create D2 jobs
        insert_job(
            sess,
            FILE_TYPE_DICT['award'],
            JOB_STATUS_DICT['finished'],
            JOB_TYPE_DICT['file_upload'],
            submission.submission_id
        )
        insert_job(
            sess,
            FILE_TYPE_DICT['award'],
            JOB_STATUS_DICT['invalid'],
            JOB_TYPE_DICT['csv_record_validation'],
            submission.submission_id
        )
        # Create dependency
        exec_comp_dep = JobDependency(
            job_id=exec_comp_job.job_id,
            prerequisite_id=award_roc_val_job.job_id
        )
        sess.add(exec_comp_dep)
        sess.commit()
