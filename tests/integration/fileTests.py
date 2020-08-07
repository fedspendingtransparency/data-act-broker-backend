import os

from datetime import datetime, timedelta

from dataactbroker.handlers.submission_handler import populate_submission_error_info

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB

from dataactcore.models.jobModels import (Submission, Job, JobDependency, CertifyHistory, PublishHistory,
                                          PublishedFilesHistory, SubmissionWindowSchedule)
from dataactcore.models.errorModels import ErrorMetadata, File
from dataactcore.models.userModel import User
from dataactcore.models.lookups import (PUBLISH_STATUS_DICT, ERROR_TYPE_DICT, RULE_SEVERITY_DICT,
                                        FILE_STATUS_DICT, FILE_TYPE_DICT, JOB_TYPE_DICT, JOB_STATUS_DICT)

from dataactvalidator.health_check import create_app

from sqlalchemy import or_
from tests.unit.dataactcore.factories.job import SubmissionFactory
from tests.integration.baseTestAPI import BaseTestAPI
from tests.integration.integration_test_helper import insert_submission, insert_job

AWARD_FILE_T = ('award_financial', 'award_financial.csv',
                open('tests/integration/data/awardFinancialValid.csv', 'rb').read())
APPROP_FILE_T = ('appropriations', 'appropriations.csv',
                 open('tests/integration/data/appropValid.csv', 'rb').read())
PA_FILE_T = ('program_activity', 'program_activity.csv',
             open('tests/integration/data/programActivityValid.csv', 'rb').read())
INVAL_FILE = ('program_activity', 'invalid_file_format.md',
              open('tests/integration/data/invalid_file_format.md', 'rb').read())


class FileTests(BaseTestAPI):
    """Test file submission routes."""

    updateSubmissionId = None
    filesSubmitted = False
    submitFilesResponse = None

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources (test data)"""
        super(FileTests, cls).setUpClass()
        # TODO: refactor into a pytest fixture

        with create_app().app_context():
            # get the submission test user
            sess = GlobalDB.db().session
            cls.session = sess
            submission_user = sess.query(User).filter(User.email == cls.test_users['admin_user']).one()
            cls.submission_user_id = submission_user.user_id

            other_user = sess.query(User).filter(User.email == cls.test_users['agency_user']).one()
            cls.other_user_email = other_user.email
            cls.other_user_id = other_user.user_id

            # setup submission/jobs data for test_check_status
            cls.status_check_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                               start_date='10/2015', end_date='12/2015',
                                                               is_quarter=True)

            cls.jobIdDict = cls.setup_jobs_for_status_check(sess, cls.status_check_submission_id)

            # setup submission/jobs data for test_error_report
            cls.error_report_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                               start_date='10/2015', end_date='10/2015')
            cls.setup_jobs_for_reports(sess, cls.error_report_submission_id)

            # setup file status data for test_metrics
            cls.test_metrics_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                               start_date='08/2015', end_date='08/2015')
            cls.setup_file_data(sess, cls.test_metrics_submission_id)

            cls.row_error_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                            start_date='10/2015', end_date='12/2015', is_quarter=True,
                                                            number_of_errors=1)
            cls.setup_submission_with_error(sess, cls.row_error_submission_id)

            cls.test_delete_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                              start_date='07/2015', end_date='09/2015', is_quarter=True)
            cls.setup_file_generation_submission(sess, cls.test_delete_submission_id)

            cls.test_published_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                                 start_date='07/2015', end_date='09/2015',
                                                                 is_quarter=True, number_of_errors=0,
                                                                 publish_status_id=2)

            cls.test_updated_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                               start_date='07/2016', end_date='09/2016',
                                                               is_quarter=True, number_of_errors=0,
                                                               publish_status_id=3)

            cls.test_reverting_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                                 start_date='07/2020', end_date='09/2020',
                                                                 is_quarter=True, number_of_errors=0,
                                                                 publish_status_id=PUBLISH_STATUS_DICT['reverting'])

            cls.test_unpublished_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                                   start_date='04/2015', end_date='06/2015',
                                                                   is_quarter=True, number_of_errors=0)

            cls.test_revalidate_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                                  start_date='10/2015', end_date='12/2015',
                                                                  is_quarter=True, number_of_errors=0)

            cls.test_monthly_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                               start_date='04/2015', end_date='06/2015',
                                                               is_quarter=False, number_of_errors=0)

            cls.dup_test_monthly_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                                   start_date='04/2015', end_date='06/2015',
                                                                   is_quarter=False, number_of_errors=0)

            cls.test_monthly_pub_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                                   start_date='09/2014', end_date='10/2014',
                                                                   is_quarter=False, number_of_errors=0,
                                                                   publish_status_id=PUBLISH_STATUS_DICT['published'])
            cls.test_monthly_cert_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                                    start_date='10/2014', end_date='11/2014',
                                                                    is_quarter=False, number_of_errors=0,
                                                                    publish_status_id=PUBLISH_STATUS_DICT['published'],
                                                                    certified=True)

            cls.test_test_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                            start_date='10/2015', end_date='12/2015',
                                                            is_quarter=False, number_of_errors=0,
                                                            test_submission=True)

            cls.test_fabs_submission_id = insert_submission(sess, cls.submission_user_id, cgac_code='SYS',
                                                            start_date='10/2015', end_date='12/2015', is_quarter=False,
                                                            number_of_errors=0, is_fabs=True)

            cls.test_other_user_submission_id = insert_submission(sess, cls.other_user_id, cgac_code='NOT',
                                                                  start_date='10/2015', end_date='12/2015',
                                                                  is_quarter=True, number_of_errors=0)
            for job_type in ['file_upload', 'csv_record_validation']:
                for file_type in ['appropriations', 'program_activity', 'award_financial']:
                    insert_job(sess, FILE_TYPE_DICT[file_type], FILE_STATUS_DICT['complete'], JOB_TYPE_DICT[job_type],
                               cls.test_other_user_submission_id)
            insert_job(sess, None, FILE_STATUS_DICT['complete'], JOB_TYPE_DICT['validation'],
                       cls.test_other_user_submission_id)

            cls.test_certify_history_id, cls.test_publish_history_id = cls.setup_publication_history(sess)

    def setUp(self):
        """Test set-up."""
        super(FileTests, self).setUp()
        self.login_admin_user()

    def call_file_submission(self):
        """Call the broker file submission route."""
        if not self.filesSubmitted:
            if CONFIG_BROKER['use_aws']:
                self.filenames = {'cgac_code': 'SYS', 'frec_code': None,
                                  'reporting_period_start_date': '01/2001',
                                  'reporting_period_end_date': '03/2001', 'is_quarter': True}
            else:
                # If local must use full destination path
                self.filenames = {'cgac_code': 'SYS', 'frec_code': None,
                                  'reporting_period_start_date': '01/2001',
                                  'reporting_period_end_date': '03/2001', 'is_quarter': 'true'}
            self.submitFilesResponse = self.app.post('/v1/upload_dabs_files/', self.filenames,
                                                     upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                                     headers={'x-session-id': self.session_id})
            self.updateSubmissionId = self.submitFilesResponse.json['submission_id']
        return self.submitFilesResponse

    def test_file_submission(self):
        """Test broker file submission and response."""
        response = self.call_file_submission()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get('Content-Type'), 'application/json')
        self.assertIn('submission_id', response.json)

    def test_test_submission(self):
        """ Test creating an explicit test submission. """
        test_submission_json = {
            'cgac_code': 'NOT',
            'frec_code': None,
            'is_quarter': True,
            'test_submission': True,
            'reporting_period_start_date': '10/2015',
            'reporting_period_end_date': '12/2015'}
        response = self.app.post('/v1/upload_dabs_files/', test_submission_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                 headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)

        sess = GlobalDB.db().session
        submission_id = response.json['submission_id']
        submission = sess.query(Submission).filter(Submission.submission_id == submission_id).one()
        self.assertEqual(submission.test_submission, True)

    def test_update_submission(self):
        """ Test upload_dabs_files with an existing submission ID """
        self.call_file_submission()
        # note: this is a quarterly test submission, so updated dates must still reflect a quarter
        file_path = 'updated.csv' if CONFIG_BROKER['use_aws'] else os.path.join(CONFIG_BROKER['broker_files'],
                                                                                'updated.csv')
        update_json = {'existing_submission_id': self.updateSubmissionId,
                       'award_financial': file_path,
                       'reporting_period_start_date': '04/2016',
                       'reporting_period_end_date': '06/2016',
                       'is_quarter': True}

        # Mark submission as published
        with create_app().app_context():
            sess = GlobalDB.db().session
            update_submission = sess.query(Submission).filter(Submission.submission_id == self.updateSubmissionId).one()
            update_submission.publish_status_id = PUBLISH_STATUS_DICT['published']
            sess.commit()
            update_response = self.app.post('/v1/upload_dabs_files/', update_json,
                                            upload_files=[('award_financial', file_path,
                                                           open('tests/integration/data/awardFinancialValid.csv',
                                                                'rb').read())],
                                            headers={'x-session-id': self.session_id})
            self.assertEqual(update_response.status_code, 200)
            self.assertEqual(update_response.headers.get('Content-Type'), 'application/json')

            submission_id = update_response.json['submission_id']
            submission = sess.query(Submission).filter(Submission.submission_id == submission_id).one()
            self.assertEqual(submission.cgac_code, 'SYS')  # Should not have changed agency name
            self.assertEqual(submission.reporting_start_date.strftime('%m/%Y'), '04/2016')
            self.assertEqual(submission.reporting_end_date.strftime('%m/%Y'), '06/2016')
            self.assertEqual(submission.publish_status_id, PUBLISH_STATUS_DICT['updated'])
            self.assertEqual(submission.test_submission, False)

    def test_bad_file_type(self):
        """ Test file submissions for bad file formats (not CSV or TXT) """
        update_json = {'existing_submission_id': self.status_check_submission_id}
        update_response = self.app.post('/v1/upload_dabs_files/', update_json,
                                        upload_files=[INVAL_FILE],
                                        headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(update_response.status_code, 400)
        self.assertEqual(update_response.json['message'], 'All submitted files must be CSV or TXT format')

    def test_upload_dabs_files_reverting_status_submission(self):
        """ Test file submissions for submissions that are currently publishing or reverting """
        update_json = {'existing_submission_id': self.test_reverting_submission_id}
        update_response = self.app.post('/v1/upload_dabs_files/', update_json,
                                        upload_files=[AWARD_FILE_T],
                                        headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(update_response.status_code, 400)
        self.assertEqual(update_response.json['message'], 'Existing submission must not be publishing, certifying, or'
                                                          ' reverting')

    def test_bad_quarter(self):
        """ Test file submissions for Q5 """
        update_json = {
            'cgac_code': '020',
            'is_quarter': True,
            'reporting_period_start_date': '12/2016',
            'reporting_period_end_date': '13/2016'}
        update_response = self.app.post('/v1/upload_dabs_files/', update_json,
                                        upload_files=[AWARD_FILE_T],
                                        headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(update_response.status_code, 400)
        self.assertIn('Date must be provided as', update_response.json['message'])

    def test_bad_month(self):
        """ Test file submissions for alphabet months """
        update_json = {
            # make sure date checks work as expected for an existing submission
            'existing_submission_id': self.status_check_submission_id,
            'reporting_period_start_date': 'AB/2016',
            'reporting_period_end_date': 'CD/2016'}
        update_response = self.app.post('/v1/upload_dabs_files/', update_json,
                                        upload_files=[AWARD_FILE_T],
                                        headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(update_response.status_code, 400)
        self.assertIn('Date must be provided as', update_response.json['message'])

    def test_bad_year(self):
        """ Test file submissions for alphabet year """
        update_json = {
            'cgac_code': '020',
            'frec_code': None,
            'is_quarter': True,
            'reporting_period_start_date': 'Q1/ABCD',
            'reporting_period_end_date': 'Q2/2016'}
        update_response = self.app.post('/v1/upload_dabs_files/', update_json,
                                        upload_files=[AWARD_FILE_T],
                                        headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(update_response.status_code, 400)
        self.assertIn('Date must be provided as', update_response.json['message'])

    def test_submit_file_published_period(self):
        """ Test file submissions for Q4, 2015, submission with same period already been published """
        update_json = {
            'cgac_code': 'SYS',
            'frec_code': None,
            'is_quarter': True,
            'reporting_period_start_date': '07/2015',
            'reporting_period_end_date': '09/2015'}
        response = self.app.post('/v1/upload_dabs_files/', update_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                 headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)

    def test_submit_file_fabs_dabs_route(self):
        """ Test trying to update a FABS submission via the DABS route """
        update_json = {
            'existing_submission_id': self.test_fabs_submission_id,
            'is_quarter': True,
            'reporting_period_start_date': '07/2015',
            'reporting_period_end_date': '09/2015'}
        response = self.app.post('/v1/upload_dabs_files/', update_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                 headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Existing submission must be a DABS submission')

    def test_submit_file_duplicate_running(self):
        """ Test trying to upload an already running FABS submission """
        insert_job(
            self.session,
            filetype=FILE_TYPE_DICT['award'],
            status=JOB_STATUS_DICT['running'],
            type_id=JOB_TYPE_DICT['file_upload'],
            submission=self.status_check_submission_id
        )
        update_json = {'existing_submission_id': self.status_check_submission_id}

        response = self.app.post('/v1/upload_dabs_files/', update_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                 headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Submission already has a running job')

    def test_submit_file_new_missing_params(self):
        """ Test file submission for a new submission while missing any of the parameters """
        update_json = {
            'cgac_code': 'TEST',
            'is_quarter': True,
            'reporting_period_start_date': '07/2015',
            'reporting_period_end_date': '09/2015'}
        response = self.app.post('/v1/upload_dabs_files/', update_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T],
                                 headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Must include all files for a new submission')

    def test_submit_file_old_no_params(self):
        """ Test file submission for an existing submission while not providing any file parameters """
        update_json = {
            'existing_submission_id': self.status_check_submission_id,
            'is_quarter': True,
            'reporting_period_start_date': '07/2015',
            'reporting_period_end_date': '09/2015'}
        response = self.app.post('/v1/upload_dabs_files/', update_json,
                                 headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Request must be a multipart/form-data type')

    def test_submit_file_wrong_permissions_wrong_user(self):
        self.login_user()
        new_submission_json = {
            'cgac_code': 'NOT',
            'frec_code': None,
            'is_quarter': True,
            'reporting_period_start_date': '07/2015',
            'reporting_period_end_date': '09/2015'}
        response = self.app.post('/v1/upload_dabs_files/', new_submission_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                 headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json['message'], 'User does not have permissions to write to that agency')

    def test_submit_file_wrong_permissions_right_user(self):
        self.login_user(username=self.other_user_email)
        update_submission_json = {
            'existing_submission_id': self.test_other_user_submission_id,
            'is_quarter': True,
            'reporting_period_start_date': '10/2015',
            'reporting_period_end_date': '12/2015'}
        response = self.app.post('/v1/upload_dabs_files/', update_submission_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                 headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 200)

    def test_submit_file_missing_parameters(self):
        self.login_user(username=self.other_user_email)
        update_submission_json = {
            'is_quarter': True,
            'reporting_period_start_date': '10/2015',
            'reporting_period_end_date': '12/2015'}
        response = self.app.post('/v1/upload_dabs_files/', update_submission_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                 headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'],
                         'Missing required parameter: cgac_code, frec_code, or existing_submission_id')

    def test_submit_file_incorrect_parameters(self):
        self.login_user(username=self.other_user_email)
        update_submission_json = {
            'existing_submission_id': -99,
            'is_quarter': True,
            'reporting_period_start_date': '10/2015',
            'reporting_period_end_date': '12/2015'}
        response = self.app.post('/v1/upload_dabs_files/', update_submission_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                 headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'existing_submission_id must be a valid submission_id')

    def test_submit_file_monthly_submission(self):
        # Single month
        monthly_submission_json = {
            'cgac_code': 'NOT',
            'frec_code': None,
            'is_quarter': False,
            'reporting_period_start_date': '05/2015',
            'reporting_period_end_date': '05/2015'}
        response = self.app.post('/v1/upload_dabs_files/', monthly_submission_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                 headers={'x-session-id': self.session_id}, expect_errors=False)
        self.assertEqual(response.status_code, 200)

        # Period 2 (with period 1)
        monthly_submission_json = {
            'cgac_code': 'NOT',
            'frec_code': None,
            'is_quarter': False,
            'reporting_period_start_date': '10/2015',
            'reporting_period_end_date': '11/2015'}
        response = self.app.post('/v1/upload_dabs_files/', monthly_submission_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                 headers={'x-session-id': self.session_id}, expect_errors=False)
        self.assertEqual(response.status_code, 200)

    def test_submit_file_monthly_submission_wrong_dates(self):
        # wrong month
        monthly_submission_json = {
            'cgac_code': 'NOT',
            'frec_code': None,
            'is_quarter': False,
            'reporting_period_start_date': '03/2015',
            'reporting_period_end_date': '04/2015'}
        response = self.app.post('/v1/upload_dabs_files/', monthly_submission_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                 headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'A monthly submission must be exactly one month with the exception'
                                                   ' of periods 1 and 2, which must be selected together.')

        # wrong year
        monthly_submission_json = {
            'cgac_code': 'NOT',
            'frec_code': None,
            'is_quarter': False,
            'reporting_period_start_date': '05/2015',
            'reporting_period_end_date': '05/2016'}
        response = self.app.post('/v1/upload_dabs_files/', monthly_submission_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                 headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'A monthly submission must be exactly one month with the exception'
                                                   ' of periods 1 and 2, which must be selected together.')

        # Period 1
        monthly_submission_json = {
            'cgac_code': 'NOT',
            'frec_code': None,
            'is_quarter': False,
            'reporting_period_start_date': '10/2015',
            'reporting_period_end_date': '10/2016'}
        response = self.app.post('/v1/upload_dabs_files/', monthly_submission_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                 headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'A monthly submission must be exactly one month with the exception'
                                                   ' of periods 1 and 2, which must be selected together.')

        # Period 2 (without period 1)
        monthly_submission_json = {
            'cgac_code': 'NOT',
            'frec_code': None,
            'is_quarter': False,
            'reporting_period_start_date': '11/2015',
            'reporting_period_end_date': '11/2016'}
        response = self.app.post('/v1/upload_dabs_files/', monthly_submission_json,
                                 upload_files=[AWARD_FILE_T, APPROP_FILE_T, PA_FILE_T],
                                 headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'A monthly submission must be exactly one month with the exception'
                                                   ' of periods 1 and 2, which must be selected together.')

    def test_revalidation_threshold_no_login(self):
        """ Test response with no login """
        self.logout()
        response = self.app.get('/v1/revalidation_threshold/', None, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 401)

    def test_revalidation_threshold(self):
        """ Test revalidation threshold route response. """
        self.login_user()
        response = self.app.get('/v1/revalidation_threshold/', None, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)

    def test_submission_metadata_no_login(self):
        """ Test response with no login """
        self.logout()
        params = {'submission_id': self.status_check_submission_id}
        response = self.app.get('/v1/submission_metadata/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 401)

    def test_submission_metadata_permission(self):
        """ Test that other users do not have access to status check submission """
        params = {'submission_id': self.status_check_submission_id}
        # Log in as non-admin user
        self.login_user()
        response = self.app.get('/v1/submission_metadata/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 403)

    def test_submission_metadata_admin(self):
        """ Test that admins have access to other user's submissions """
        params = {'submission_id': self.status_check_submission_id}
        # Log in as admin user
        self.login_admin_user()
        response = self.app.get('/v1/submission_metadata/', params, headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)

    def test_submission_metadata(self):
        """ Test submission_metadata route response. """
        params = {'submission_id': self.status_check_submission_id}
        response = self.app.get('/v1/submission_metadata/', params, headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)

        # Make sure we got the right submission
        json = response.json
        self.assertEqual(json['cgac_code'], 'SYS')
        self.assertEqual(json['reporting_period'], 'Q1/2016')

    def test_submission_data_no_login(self):
        """ Test response with no login """
        self.logout()
        params = {'submission_id': self.status_check_submission_id}
        response = self.app.get('/v1/submission_data/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 401)

    def test_submission_data_invalid_file_type(self):
        """ Test response with a completely invalid file type """
        self.logout()
        params = {'submission_id': self.status_check_submission_id, 'type': 'approp'}
        response = self.app.get('/v1/submission_data/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 401)

    def test_submission_data_bad_file_type(self):
        """ Test response with a real file type requested but invalid for this submission """
        self.logout()
        params = {'submission_id': self.status_check_submission_id, 'type': 'fabs'}
        response = self.app.get('/v1/submission_data/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 401)

    def test_submission_data_permission(self):
        """ Test that other users do not have access to status check submission """
        params = {'submission_id': self.status_check_submission_id}
        # Log in as non-admin user
        self.login_user()
        response = self.app.get('/v1/submission_data/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 403)

    def test_submission_data_admin(self):
        """ Test that admins have access to other user's submissions """
        params = {'submission_id': self.status_check_submission_id}
        # Log in as admin user
        self.login_admin_user()
        response = self.app.get('/v1/submission_data/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)

    def test_submission_data_invalid_type(self):
        """ Test that an invalid file type to check status returns an error """
        params = {'submission_id': self.status_check_submission_id, 'type': 'approp'}
        response = self.app.get('/v1/submission_data/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        # Assert 400 status
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'approp is not a valid file type')

    def test_submission_data_type_param(self):
        """ Test broker status route response with case-ignored type argument. """
        params = {'submission_id': self.status_check_submission_id, 'type': 'apPropriations'}
        response = self.app.get('/v1/submission_data/', params, headers={'x-session-id': self.session_id})

        self.assertEqual(response.status_code, 200, msg=str(response.json))
        self.assertEqual(response.headers.get('Content-Type'), 'application/json')
        json = response.json

        # create list of all file types including cross other than fabs
        self.assertEqual(len(json['jobs']), 1)
        self.assertEqual(json['jobs'][0]['file_type'], 'appropriations')

    def test_submission_data(self):
        """ Test submission_data route response. """
        params = {'submission_id': self.status_check_submission_id}

        # Populate error data and make sure we're getting the right contents
        with create_app().app_context():
            populate_submission_error_info(self.status_check_submission_id)
            response = self.app.get('/v1/submission_data/', params, expect_errors=True,
                                    headers={'x-session-id': self.session_id})
            self.assertEqual(response.status_code, 200, msg=str(response.json))
            self.assertEqual(response.headers.get('Content-Type'), 'application/json')
            json = response.json
            # response ids are coming back as string, so patch the jobIdDict
            job_id_dict = {k: str(self.jobIdDict[k]) for k in self.jobIdDict.keys()}
            job_list = json['jobs']
            approp_job = None
            cross_job = None
            for job in job_list:
                if str(job['job_id']) == str(job_id_dict['appropriations']):
                    # Found the job to be checked
                    approp_job = job
                elif str(job['job_id']) == str(job_id_dict['cross_file']):
                    # Found cross file job
                    cross_job = job

            # Must have an approp job and cross-file job
            self.assertNotEqual(approp_job, None)
            self.assertNotEqual(cross_job, None)
            # And that job must have the following
            self.assertEqual(approp_job['job_status'], 'ready')
            self.assertEqual(approp_job['job_type'], 'csv_record_validation')
            self.assertEqual(approp_job['file_type'], 'appropriations')
            self.assertEqual(approp_job['filename'], 'approp.csv')
            self.assertEqual(approp_job['file_status'], 'complete')
            self.assertIn('missing_header_one', approp_job['missing_headers'])
            self.assertIn('missing_header_two', approp_job['missing_headers'])
            self.assertIn('duplicated_header_one', approp_job['duplicated_headers'])
            self.assertIn('duplicated_header_two', approp_job['duplicated_headers'])
            # Check file size and number of rows
            self.assertEqual(approp_job['file_size'], 2345)
            self.assertEqual(approp_job['number_of_rows'], 566)

            # Check error metadata for specified error
            rule_error_data = None
            for data in approp_job['error_data']:
                if data['field_name'] == 'header_three':
                    rule_error_data = data
            self.assertIsNotNone(rule_error_data)
            self.assertEqual(rule_error_data['field_name'], 'header_three')
            self.assertEqual(rule_error_data['error_name'], 'rule_failed')
            self.assertEqual(rule_error_data['error_description'], 'A rule failed for this value.')
            self.assertEqual(rule_error_data['occurrences'], '7')
            self.assertEqual(rule_error_data['rule_failed'], 'Header three value must be real')
            self.assertEqual(rule_error_data['original_label'], 'A1')
            # Check warning metadata for specified warning
            warning_error_data = None
            for data in approp_job['warning_data']:
                if data['field_name'] == 'header_three':
                    warning_error_data = data
            self.assertIsNotNone(warning_error_data)
            self.assertEqual(warning_error_data['field_name'], 'header_three')
            self.assertEqual(warning_error_data['error_name'], 'rule_failed')
            self.assertEqual(warning_error_data['error_description'], 'A rule failed for this value.')
            self.assertEqual(warning_error_data['occurrences'], '7')
            self.assertEqual(warning_error_data['rule_failed'], 'Header three value looks odd')
            self.assertEqual(warning_error_data['original_label'], 'A2')

            rule_error_data = None
            for data in cross_job['error_data']:
                if data['field_name'] == 'header_four':
                    rule_error_data = data

            self.assertEqual(rule_error_data['source_file'], 'appropriations')
            self.assertEqual(rule_error_data['target_file'], 'award')

    def test_check_status_no_login(self):
        """ Test response with no login """
        self.logout()
        params = {'submission_id': self.status_check_submission_id}
        response = self.app.get('/v1/check_status/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        # Assert 401 status
        self.assertEqual(response.status_code, 401)

    def test_check_status_no_session_id(self):
        """ Test response with no session ID """
        params = {'submission_id': self.status_check_submission_id}
        response = self.app.get('/v1/check_status/', params, expect_errors=True)
        # Assert 401 status
        self.assertEqual(response.status_code, 401)

    def test_check_status_permission(self):
        """ Test that other users do not have access to status check submission """
        params = {'submission_id': self.status_check_submission_id}
        # Log in as non-admin user
        self.login_user()
        # Call check status route
        response = self.app.get('/v1/check_status/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        # Assert 400 status
        self.assertEqual(response.status_code, 403)

    def test_check_status_admin(self):
        """ Test that admins have access to other user's submissions """
        params = {'submission_id': self.status_check_submission_id}
        # Log in as admin user
        self.login_admin_user()
        # Call check status route (also checking case insensitivity of header here)
        response = self.app.get('/v1/check_status/', params, expect_errors=True,
                                headers={'x-SESSION-id': self.session_id})
        # Assert 200 status
        self.assertEqual(response.status_code, 200)

    def test_check_status(self):
        """ Test broker status route response. """
        params = {'submission_id': self.status_check_submission_id}
        response = self.app.get('/v1/check_status/', params, headers={'x-session-id': self.session_id})

        self.assertEqual(response.status_code, 200, msg=str(response.json))
        self.assertEqual(response.headers.get('Content-Type'), 'application/json')
        json = response.json

        # create list of all file types including cross other than fabs
        file_type_keys = {k if k != 'fabs' else 'cross' for k in FILE_TYPE_DICT}
        response_keys = {k for k in json.keys()}
        self.assertEqual(file_type_keys, response_keys)

    def test_check_status_invalid_type(self):
        """ Test that an invalid file type to check status returns an error """
        params = {'submission_id': self.status_check_submission_id, 'type': 'approp'}
        response = self.app.get('/v1/check_status/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        # Assert 400 status
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'approp is not a valid file type')

    def test_check_status_type_param(self):
        """ Test broker status route response with case-ignored type argument. """
        params = {'submission_id': self.status_check_submission_id, 'type': 'apPropriations'}
        response = self.app.get('/v1/check_status/', params, headers={'x-session-id': self.session_id})

        self.assertEqual(response.status_code, 200, msg=str(response.json))
        self.assertEqual(response.headers.get('Content-Type'), 'application/json')
        json = response.json

        # create list of all file types including cross other than fabs
        response_keys = {k for k in json.keys()}
        self.assertEqual(len(response_keys), 1)
        self.assertEqual({'appropriations'}, response_keys)

    def test_get_obligations(self):
        """ Test submission obligations with an existing Submission """
        submission = SubmissionFactory()
        self.session.add(submission)
        self.session.commit()
        response = self.app.get('/v1/get_obligations/', {'submission_id': submission.submission_id},
                                headers={'x-session-id': self.session_id})
        assert response.status_code == 200
        assert 'total_obligations' in response.json

    def check_metrics(self, submission_id, exists, type_file):
        """Get error metrics for specified submission."""
        post_json = {'submission_id': submission_id}
        response = self.app.post_json('/v1/error_metrics/', post_json, headers={'x-session-id': self.session_id})

        self.assertEqual(response.status_code, 200)

        type_file_length = len(response.json[type_file])
        if exists:
            self.assertGreater(type_file_length, 0)
        else:
            self.assertEqual(type_file_length, 0)

    def test_metrics(self):
        """Test broker status record handling."""
        # Check the route
        self.check_metrics(self.test_metrics_submission_id, False, 'award')
        self.check_metrics(self.test_metrics_submission_id, True, 'award_financial')
        self.check_metrics(self.test_metrics_submission_id, True, 'appropriations')

    def test_delete_submission(self):
        sess = GlobalDB.db().session
        jobs_orig = sess.query(Job).filter(Job.submission_id == self.test_delete_submission_id).all()
        job_ids = [job.job_id for job in jobs_orig]

        post_json = {'submission_id': self.test_delete_submission_id}
        response = self.app.post_json('/v1/delete_submission/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(response.json['message'], 'Success')

        response = self.app.get('/v1/check_status/', post_json, headers={'x-session-id': self.session_id},
                                expect_errors=True)
        self.assertEqual(response.json['message'], 'No such submission')

        # check if models were actually delete (verifying cascading worked)
        jobs_new = sess.query(Job).filter(Job.submission_id == self.test_delete_submission_id).all()
        self.assertEqual(jobs_new, [])

        job_deps = sess.query(JobDependency).filter(or_(JobDependency.job_id.in_(job_ids),
                                                        JobDependency.prerequisite_id.in_(job_ids))).all()
        self.assertEqual(job_deps, [])

        # test trying to delete a published submission (failure expected)
        post_json = {'submission_id': self.test_published_submission_id}
        response = self.app.post_json('/v1/delete_submission/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.json['message'], 'Submissions that have been published cannot be deleted')

        # test trying to delete an updated submission (failure expected)
        post_json = {'submission_id': self.test_updated_submission_id}
        response = self.app.post_json('/v1/delete_submission/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.json['message'], 'Submissions that have been published cannot be deleted')

    def test_published_submission_ids_empty(self):
        params = {'cgac_code': 'SYS',
                  'submission_id': '1',
                  'reporting_fiscal_year': '2015',
                  'reporting_fiscal_period': '3'}
        response = self.app.get('/v1/published_submissions/', params, headers={'x-session-id': self.session_id},
                                expect_errors=False)
        self.assertEqual(response.json['published_submissions'], [])

    def test_published_submission_ids_populated(self):
        params = {'cgac_code': 'SYS',
                  'submission_id': '1',
                  'reporting_fiscal_year': '2015',
                  'reporting_fiscal_period': '12'}

        response = self.app.get('/v1/published_submissions/', params, headers={'x-session-id': self.session_id},
                                expect_errors=False)
        self.assertEqual(response.json['published_submissions'][0]['submission_id'], self.test_published_submission_id)

    def test_published_submission_ids_updated(self):
        params = {'cgac_code': 'SYS',
                  'submission_id': '1',
                  'reporting_fiscal_year': '2016',
                  'reporting_fiscal_period': '12'}

        response = self.app.get('/v1/published_submissions/', params, headers={'x-session-id': self.session_id},
                                expect_errors=False)
        self.assertEqual(response.json['published_submissions'][0]['submission_id'], self.test_updated_submission_id)

    def test_publish_and_certify_dabs_submission(self):
        post_json = {'submission_id': self.row_error_submission_id}
        response = self.app.post_json('/v1/publish_and_certify_dabs_submission/', post_json,
                                      headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.json['message'], 'Submission cannot be published due to critical errors')

        post_json = {'submission_id': self.test_test_submission_id}
        response = self.app.post_json('/v1/publish_and_certify_dabs_submission/', post_json,
                                      headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.json['message'], 'Test submissions cannot be published')

        post_json = {'submission_id': self.test_published_submission_id}
        response = self.app.post_json('/v1/publish_and_certify_dabs_submission/', post_json,
                                      headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.json['message'], 'Submission has already been published')

        insert_job(self.session, FILE_TYPE_DICT['appropriations'], JOB_STATUS_DICT['finished'],
                   JOB_TYPE_DICT['csv_record_validation'], self.test_unpublished_submission_id, num_valid_rows=0)
        test_sub = self.session.query(Submission).filter_by(submission_id=self.test_unpublished_submission_id).one()
        submission_window = SubmissionWindowSchedule(year=test_sub.reporting_fiscal_year,
                                                     period=test_sub.reporting_fiscal_period,
                                                     period_start=datetime.now() - timedelta(days=1))
        self.session.add(submission_window)
        self.session.commit()
        post_json = {'submission_id': self.test_unpublished_submission_id}
        response = self.app.post_json('/v1/publish_and_certify_dabs_submission/', post_json,
                                      headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.json['message'], 'Cannot publish while file A or B is blank.')

        # ensuring monthly submissions can be certified
        insert_job(self.session, FILE_TYPE_DICT['appropriations'], JOB_STATUS_DICT['finished'],
                   JOB_TYPE_DICT['csv_record_validation'], self.test_monthly_submission_id, num_valid_rows=1)
        insert_job(self.session, FILE_TYPE_DICT['program_activity'], JOB_STATUS_DICT['finished'],
                   JOB_TYPE_DICT['csv_record_validation'], self.test_monthly_submission_id, num_valid_rows=1)
        insert_job(self.session, None, JOB_STATUS_DICT['finished'],
                   JOB_TYPE_DICT['validation'], self.test_monthly_submission_id, num_valid_rows=1)
        post_json = {'submission_id': self.test_monthly_submission_id}
        response = self.app.post_json('/v1/publish_and_certify_dabs_submission/', post_json,
                                      headers={'x-session-id': self.session_id}, expect_errors=False)
        self.assertEqual(response.status_code, 200)

        # prevent submission from certifying if there are already certified submissions in the period
        insert_job(self.session, FILE_TYPE_DICT['appropriations'], JOB_STATUS_DICT['finished'],
                   JOB_TYPE_DICT['csv_record_validation'], self.dup_test_monthly_submission_id, num_valid_rows=1)
        insert_job(self.session, FILE_TYPE_DICT['program_activity'], JOB_STATUS_DICT['finished'],
                   JOB_TYPE_DICT['csv_record_validation'], self.dup_test_monthly_submission_id, num_valid_rows=1)
        insert_job(self.session, None, JOB_STATUS_DICT['finished'],
                   JOB_TYPE_DICT['validation'], self.dup_test_monthly_submission_id, num_valid_rows=1)
        post_json = {'submission_id': self.dup_test_monthly_submission_id}
        response = self.app.post_json('/v1/publish_and_certify_dabs_submission/', post_json,
                                      headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.json['message'], 'Test submissions cannot be published')

        # Testing the double check
        dup_test_sub = self.session.query(Submission).filter_by(submission_id=self.dup_test_monthly_submission_id).one()
        dup_test_sub.test_submission = False
        self.session.commit()
        post_json = {'submission_id': self.dup_test_monthly_submission_id}
        response = self.app.post_json('/v1/publish_and_certify_dabs_submission/', post_json,
                                      headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.json['message'], 'This period already has published submission(s) by this agency.')

    def test_publish_dabs_submission(self):
        """ Tests the publish_dabs_submission endpoint"""
        # Quarterly submissions cannot be published/certified separately
        post_json = {'submission_id': self.test_published_submission_id}
        response = self.app.post_json('/v1/publish_dabs_submission/', post_json,
                                      headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.json['message'], 'Quarterly submissions cannot be published separate from'
                                                   ' certification. Use the publish_and_certify_dabs_submission'
                                                   ' endpoint to publish and certify.')

        # Submissions that have been certified cannot be published individually again
        post_json = {'submission_id': self.test_monthly_cert_submission_id}
        response = self.app.post_json('/v1/publish_dabs_submission/', post_json,
                                      headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.json['message'], 'Submissions that have been certified cannot be republished'
                                                   ' separately. Use the publish_and_certify_dabs_submission endpoint'
                                                   ' to republish.')

    def test_certify_dabs_submission(self):
        """ Tests the certify_dabs_submission endpoint"""
        # Quarterly submissions cannot be published/certified separately
        post_json = {'submission_id': self.test_published_submission_id}
        response = self.app.post_json('/v1/certify_dabs_submission/', post_json,
                                      headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.json['message'], 'Quarterly submissions cannot be certified separate from'
                                                   ' publication. Use the publish_and_certify_dabs_submission'
                                                   ' endpoint to publish and certify.')

        # Submissions that have been certified cannot be certified individually again
        post_json = {'submission_id': self.test_monthly_cert_submission_id}
        response = self.app.post_json('/v1/certify_dabs_submission/', post_json,
                                      headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.json['message'], 'Submissions that have been certified cannot be recertified'
                                                   ' separately. Use the publish_and_certify_dabs_submission endpoint'
                                                   ' to recertify.')

        # Submissions that have not been published cannot be certified
        post_json = {'submission_id': self.dup_test_monthly_submission_id}
        response = self.app.post_json('/v1/certify_dabs_submission/', post_json,
                                      headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.json['message'], 'Submissions must be published before certification. Use the'
                                                   ' publish_dabs_submission endpoint to publish first.')

    def test_list_history(self):
        params = {'submission_id': self.test_published_submission_id}
        response = self.app.get('/v1/list_history/', params, headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertNotEqual(len(response.json['certifications']), 0)

        params = {'submission_id': self.test_fabs_submission_id}
        response = self.app.get('/v1/list_history/', params, headers={'x-session-id': self.session_id},
                                expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'FABS submissions do not have a publication history')

        params = {'submission_id': self.test_unpublished_submission_id}
        response = self.app.get('/v1/list_history/', params, headers={'x-session-id': self.session_id},
                                expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'This submission has no publication history')

    def test_get_certified_file(self):
        sess = GlobalDB.db().session
        published_files_history = sess.query(PublishedFilesHistory).\
            filter_by(certify_history_id=self.test_certify_history_id, file_type_id=FILE_TYPE_DICT['appropriations']).\
            one()
        published_files_history_d = sess.query(PublishedFilesHistory). \
            filter_by(certify_history_id=self.test_certify_history_id,
                      file_type_id=FILE_TYPE_DICT['award_procurement']). \
            one()
        published_files_history_cross = sess.query(PublishedFilesHistory). \
            filter_by(certify_history_id=self.test_certify_history_id,
                      file_type_id=None). \
            one()

        # valid warning file
        post_json = {'submission_id': self.test_published_submission_id, 'is_warning': True,
                     'published_files_history_id': published_files_history.published_files_history_id}
        response = self.app.post_json('/v1/get_certified_file/', post_json, headers={'x-session-id': self.session_id})
        self.assertIn('path/to/warning_file_a.csv', response.json['url'])
        self.assertEqual(response.status_code, 200)

        # valid uploaded file
        post_json = {'submission_id': self.test_published_submission_id, 'is_warning': False,
                     'published_files_history_id': published_files_history.published_files_history_id}
        response = self.app.post_json('/v1/get_certified_file/', post_json, headers={'x-session-id': self.session_id})
        self.assertIn('path/to/file_a.csv', response.json['url'])
        self.assertEqual(response.status_code, 200)

        # nonexistent published_files_history_id
        post_json = {'submission_id': self.test_published_submission_id, 'is_warning': False,
                     'published_files_history_id': -1}
        response = self.app.post_json('/v1/get_certified_file/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Invalid published_files_history_id')

        # non-matching submission_id and published_files_history_id
        post_json = {'submission_id': self.test_monthly_submission_id, 'is_warning': False,
                     'published_files_history_id': published_files_history.published_files_history_id}
        response = self.app.post_json('/v1/get_certified_file/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'],
                         'Requested published_files_history_id does not match submission_id provided')

        # no warning file associated with entry when requesting warning file
        post_json = {'submission_id': self.test_published_submission_id, 'is_warning': True,
                     'published_files_history_id': published_files_history_d.published_files_history_id}
        response = self.app.post_json('/v1/get_certified_file/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'History entry has no warning file')

        # no uploaded file associated with entry when requesting uploaded file
        post_json = {'submission_id': self.test_published_submission_id, 'is_warning': False,
                     'published_files_history_id': published_files_history_cross.published_files_history_id}
        response = self.app.post_json('/v1/get_certified_file/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'History entry has no related file')

    def test_revalidate_submission(self):
        post_json = {'submission_id': self.row_error_submission_id}
        response = self.app.post_json('/v1/restart_validation/', post_json,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.json['message'], 'Success')

        post_json = {'submission_id': self.test_fabs_submission_id, 'd2_submission': True}
        response = self.app.post_json('/v1/restart_validation/', post_json,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.json['message'], 'Success')

    def test_fail_revalidate_submission(self):
        """ Test that a submission cannot be revalidated while it's reverting. """
        post_json = {'submission_id': self.test_reverting_submission_id}
        response = self.app.post_json('/v1/restart_validation/', post_json,
                                      headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Submission is certifying or reverting')

    def test_submission_report_url(self):
        """ Test that the submission's report is successfully generated """
        params = {'warning': False,
                  'file_type': 'appropriations'}
        response = self.app.get('/v1/submission/{}/report_url'.format(self.row_error_submission_id), params,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertIn('url', response.json)

    def test_submission_report_url_invalid_file(self):
        """ Test that invalid file_types cause an error (even if they're technically a file type that we have, just
            not one with error reports)
        """
        params = {'warning': False,
                  'file_type': 'executive_compensation'}
        response = self.app.get('/v1/submission/{}/report_url'.format(self.row_error_submission_id), params,
                                headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file_type: Not a valid choice.')

    def test_submission_report_url_invalid_cross(self):
        """ Test that invalid cross_types cause an error """
        params = {'warning': False,
                  'file_type': 'appropriations',
                  'cross_type': 'appropriations'}
        response = self.app.get('/v1/submission/{}/report_url'.format(self.row_error_submission_id), params,
                                headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'cross_type: Not a valid choice.')

    def test_submission_report_url_valid_type_invalid_pair(self):
        """ Test that valid cross_type but invalid pair causes an error """
        params = {'warning': False,
                  'file_type': 'appropriations',
                  'cross_type': 'award'}
        response = self.app.get('/v1/submission/{}/report_url'.format(self.row_error_submission_id), params,
                                headers={'x-session-id': self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'appropriations and award is not a valid cross-pair.')

    def test_cross_file_status_reset_generate(self):
        """ Test that cross-file resets when D file generation is called. """
        submission = SubmissionFactory()
        self.session.add(submission)
        self.session.commit()

        job_1 = insert_job(self.session, FILE_TYPE_DICT['award'], JOB_STATUS_DICT['finished'],
                           JOB_TYPE_DICT['csv_record_validation'], submission.submission_id, num_errors=1)
        job_2 = insert_job(self.session, FILE_TYPE_DICT['award_procurement'], JOB_STATUS_DICT['finished'],
                           JOB_TYPE_DICT['file_upload'], submission.submission_id)
        job_3 = insert_job(self.session, FILE_TYPE_DICT['award_procurement'], JOB_STATUS_DICT['finished'],
                           JOB_TYPE_DICT['csv_record_validation'], submission.submission_id)
        job_4 = insert_job(self.session, None, JOB_STATUS_DICT['finished'], JOB_TYPE_DICT['validation'],
                           submission.submission_id)

        dep_1 = JobDependency(
            job_id=job_3.job_id,
            prerequisite_id=job_2.job_id
        )
        dep_2 = JobDependency(
            job_id=job_4.job_id,
            prerequisite_id=job_3.job_id
        )
        dep_3 = JobDependency(
            job_id=job_4.job_id,
            prerequisite_id=job_1.job_id
        )
        self.session.add_all([dep_1, dep_2, dep_3])
        self.session.commit()

        # Make sure the cross-file job is finished for real first
        cross_job = self.session.query(Job).filter(Job.job_id == job_4.job_id).one()
        self.assertEqual(cross_job.job_status_id, JOB_STATUS_DICT['finished'])

        # Call a generation to test resetting the status, make sure the call succeeded
        post_json = {'submission_id': submission.submission_id, 'file_type': 'D1',
                     'start': '01/02/2016', 'end': '02/03/2016'}
        response = self.app.post_json('/v1/generate_file/', post_json, headers={'x-session-id': self.session_id})
        # Need to commit for it to take within this session for some reason
        self.session.commit()
        self.assertEqual(response.status_code, 200)

        # Check to make sure the status changed and didn't change back (because the appropriation job isn't done, no
        # good way to make sure D file generation fails so we have to use a different job)
        cross_job = self.session.query(Job).filter(Job.job_id == job_4.job_id).one()
        self.assertEqual(cross_job.job_status_id, JOB_STATUS_DICT['waiting'])

    def test_cross_file_status_reset_new_upload(self):
        """ Test that cross-file resets when D file generation is called. """
        submission = SubmissionFactory()
        self.session.add(submission)
        self.session.commit()

        job_1 = insert_job(self.session, FILE_TYPE_DICT['appropriations'], JOB_STATUS_DICT['waiting'],
                           JOB_TYPE_DICT['file_upload'], submission.submission_id)
        job_2 = insert_job(self.session, FILE_TYPE_DICT['appropriations'], JOB_STATUS_DICT['waiting'],
                           JOB_TYPE_DICT['csv_record_validation'], submission.submission_id)
        job_3 = insert_job(self.session, FILE_TYPE_DICT['award_financial'], JOB_STATUS_DICT['waiting'],
                           JOB_TYPE_DICT['csv_record_validation'], submission.submission_id)
        job_4 = insert_job(self.session, None, JOB_STATUS_DICT['finished'], JOB_TYPE_DICT['validation'],
                           submission.submission_id)

        dep_1 = JobDependency(
            job_id=job_2.job_id,
            prerequisite_id=job_1.job_id
        )
        dep_2 = JobDependency(
            job_id=job_4.job_id,
            prerequisite_id=job_3.job_id
        )
        dep_3 = JobDependency(
            job_id=job_4.job_id,
            prerequisite_id=job_2.job_id
        )
        self.session.add_all([dep_1, dep_2, dep_3])
        self.session.commit()

        # Make sure the cross-file job is finished for real first
        cross_job = self.session.query(Job).filter(Job.job_id == job_4.job_id).one()
        self.assertEqual(cross_job.job_status_id, JOB_STATUS_DICT['finished'])

        # Call a generation to test resetting the status, make sure the call succeeded
        update_json = {'existing_submission_id': submission.submission_id,
                       'reporting_period_start_date': '04/2016',
                       'reporting_period_end_date': '06/2016',
                       'is_quarter': True}
        update_response = self.app.post('/v1/upload_dabs_files/', update_json,
                                        upload_files=[APPROP_FILE_T],
                                        headers={'x-session-id': self.session_id})
        # Need to commit for it to take within this session for some reason
        self.session.commit()
        self.assertEqual(update_response.status_code, 200)

        # Check to make sure the status changed and didn't change back (because the appropriation job isn't done, no
        # good way to make sure D file generation fails so we have to use a different job)
        cross_job = self.session.query(Job).filter(Job.job_id == job_4.job_id).one()
        self.assertEqual(cross_job.job_status_id, JOB_STATUS_DICT['waiting'])

    @staticmethod
    def insert_file(sess, job_id, status):
        """Insert one file into error database and get ID back."""
        fs = File(job_id=job_id, filename=' ', file_status_id=status)
        sess.add(fs)
        sess.commit()
        return fs.file_id

    @classmethod
    def insert_row_level_error(cls, sess, job_id):
        """Insert one error into error database."""
        ed = ErrorMetadata(
            job_id=job_id,
            filename='test.csv',
            field_name='header 1',
            error_type_id=ERROR_TYPE_DICT['type_error'],
            occurrences=100,
            first_row=123,
            rule_failed='Type Check'
        )
        sess.add(ed)
        sess.commit()
        return ed.error_metadata_id

    @classmethod
    def insert_published_files_history(cls, sess, ch_id, ph_id, submission_id, file_type=None, filename=None,
                                       warning_filename=None, comment=None):
        """ Insert one history entry into published files history database. """
        cfh = PublishedFilesHistory(
            certify_history_id=ch_id,
            publish_history_id=ph_id,
            submission_id=submission_id,
            filename=filename,
            file_type_id=file_type,
            warning_filename=warning_filename,
            comment=comment
        )
        sess.add(cfh)
        sess.commit()
        return cfh.published_files_history_id

    @classmethod
    def setup_publication_history(cls, sess):
        submission_id = cls.test_published_submission_id

        ch = CertifyHistory(
            user_id=cls.submission_user_id,
            submission_id=submission_id
        )
        ph = PublishHistory(
            user_id=cls.submission_user_id,
            submission_id=submission_id
        )
        sess.add_all([ch, ph])
        sess.commit()

        # Create an A file entry
        cls.insert_published_files_history(
            sess,
            ch.certify_history_id,
            ph.publish_history_id,
            submission_id,
            FILE_TYPE_DICT['appropriations'],
            'path/to/file_a.csv',
            'path/to/warning_file_a.csv',
            'Comment content'
        )

        # Create a D1 file entry
        cls.insert_published_files_history(
            sess,
            ch.certify_history_id,
            ph.publish_history_id,
            submission_id,
            FILE_TYPE_DICT['award_procurement'],
            'path/to/file_d1.csv'
        )

        # Create a cross-file entry
        cls.insert_published_files_history(
            sess,
            ch.certify_history_id,
            ph.publish_history_id,
            submission_id,
            warning_filename='path/to/cross_file.csv'
        )

        return ch.certify_history_id, ph.publish_history_id

    @classmethod
    def setup_file_generation_submission(cls, sess, submission_id):
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
        sub_award_job.error_message = 'File was invalid'

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

    @classmethod
    def setup_submission_with_error(cls, sess, row_error_submission_id):
        """ Set up a submission that will come back with a status of validation_errors """
        job_values = {
            'awardFin': [3, 4, 2, 'awardFin.csv', 100, 100],
            'appropriations': [1, 4, 2, 'approp.csv', 2345, 567],
            'program_activity': [2, 4, 2, 'programActivity.csv', None, None],
            'cross_file': [None, 4, 4, 2, None, None, None]
        }

        for job_key, values in job_values.items():
            job = insert_job(
                sess,
                filetype=values[0],
                status=values[1],
                type_id=values[2],
                submission=row_error_submission_id,
                original_filename=values[3],
                file_size=values[4],
                num_rows=values[5]
            )
        # Add errors to cross file job
        metadata = ErrorMetadata(
            job_id=job.job_id,
            occurrences=2,
            severity_id=RULE_SEVERITY_DICT['fatal']
        )
        sess.add(metadata)
        sess.commit()

    @classmethod
    def setup_jobs_for_status_check(cls, sess, submission_id):
        """Set up test jobs for job status test."""
        job_values = {
            'uploadFinished': [FILE_TYPE_DICT['award'], JOB_STATUS_DICT['finished'],
                               JOB_TYPE_DICT['file_upload'], None, None, None],
            'recordRunning': [FILE_TYPE_DICT['award'], JOB_STATUS_DICT['running'],
                              JOB_TYPE_DICT['csv_record_validation'], None, None, None],
            'awardFin': [FILE_TYPE_DICT['award_financial'], JOB_STATUS_DICT['ready'],
                         JOB_TYPE_DICT['csv_record_validation'], 'awardFin.csv', 100, 100],
            'appropriations': [FILE_TYPE_DICT['appropriations'], JOB_STATUS_DICT['ready'],
                               JOB_TYPE_DICT['csv_record_validation'], 'approp.csv', 2345, 567],
            'program_activity': [FILE_TYPE_DICT['program_activity'], JOB_STATUS_DICT['ready'],
                                 JOB_TYPE_DICT['csv_record_validation'], 'programActivity.csv', None, None],
            'cross_file': [None, JOB_STATUS_DICT['finished'], JOB_TYPE_DICT['validation'], 2, None, None, None]
        }
        job_id_dict = {}
        approp_job = None

        for job_key, values in job_values.items():
            job = insert_job(
                sess,
                filetype=values[0],
                status=values[1],
                type_id=values[2],
                submission=submission_id,
                original_filename=values[3],
                file_size=values[4],
                num_rows=values[5]
            )
            job_id_dict[job_key] = job.job_id
            if job_key == 'appropriations':
                approp_job = job
            elif job_key == 'cross_file':
                cross_file_job = job

        # For appropriations job, create an entry in file for this job
        file_rec = File(
            job_id=job_id_dict['appropriations'],
            filename='approp.csv',
            file_status_id=FILE_STATUS_DICT['complete'],
            headers_missing='missing_header_one, missing_header_two',
            headers_duplicated='duplicated_header_one, duplicated_header_two')
        sess.add(file_rec)

        cross_file = File(
            job_id=job_id_dict['cross_file'],
            filename='approp.csv',
            file_status_id=FILE_STATUS_DICT['complete'],
            headers_missing='',
            headers_duplicated='')
        sess.add(cross_file)

        # Put some entries in error data for approp job
        rule_error = ErrorMetadata(
            job_id=job_id_dict['appropriations'],
            filename='approp.csv',
            field_name='header_three',
            error_type_id=ERROR_TYPE_DICT['rule_failed'],
            occurrences=7,
            rule_failed='Header three value must be real',
            original_rule_label='A1',
            file_type_id=FILE_TYPE_DICT['appropriations'],
            target_file_type_id=FILE_TYPE_DICT['award'],
            severity_id=RULE_SEVERITY_DICT['fatal']
        )
        approp_job.number_of_errors += 7
        sess.add(rule_error)

        warning_error = ErrorMetadata(
            job_id=job_id_dict['appropriations'],
            filename='approp.csv',
            field_name='header_three',
            error_type_id=ERROR_TYPE_DICT['rule_failed'],
            occurrences=7,
            rule_failed='Header three value looks odd',
            original_rule_label='A2',
            file_type_id=FILE_TYPE_DICT['appropriations'],
            target_file_type_id=FILE_TYPE_DICT['award'],
            severity_id=RULE_SEVERITY_DICT['warning']
        )
        approp_job.number_of_warnings += 7
        sess.add(warning_error)

        req_error = ErrorMetadata(
            job_id=job_id_dict['appropriations'],
            filename='approp.csv',
            field_name='header_four',
            error_type_id=ERROR_TYPE_DICT['required_error'],
            occurrences=5,
            rule_failed='This field is required for all submissions but was not provided in this row.',
            severity_id=RULE_SEVERITY_DICT['fatal']
        )
        approp_job.number_of_errors += 5
        sess.add(req_error)

        cross_error = ErrorMetadata(
            job_id=job_id_dict['cross_file'],
            filename='approp.csv',
            field_name='header_four',
            error_type_id=ERROR_TYPE_DICT['required_error'],
            occurrences=5,
            rule_failed='This field is required for all submissions but was not provided in this row.',
            file_type_id=FILE_TYPE_DICT['appropriations'],
            target_file_type_id=FILE_TYPE_DICT['award'],
            severity_id=RULE_SEVERITY_DICT['fatal']
        )
        cross_file_job.number_of_errors += 5
        sess.add(cross_error)

        sess.commit()
        return job_id_dict

    @classmethod
    def setup_jobs_for_reports(cls, sess, error_report_submission_id):
        """Setup jobs table for checking validator unit test error reports."""
        finished = JOB_STATUS_DICT['finished']
        csv_validation = JOB_TYPE_DICT['csv_record_validation']
        insert_job(sess, filetype=FILE_TYPE_DICT['award'], status=finished, type_id=csv_validation,
                   submission=error_report_submission_id)
        insert_job(sess, filetype=FILE_TYPE_DICT['award_financial'], status=finished, type_id=csv_validation,
                   submission=error_report_submission_id)
        insert_job(sess, filetype=FILE_TYPE_DICT['appropriations'], status=finished, type_id=csv_validation,
                   submission=error_report_submission_id)
        insert_job(sess, filetype=FILE_TYPE_DICT['program_activity'], status=finished, type_id=csv_validation,
                   submission=error_report_submission_id)

    @classmethod
    def setup_file_data(cls, sess, submission_id):
        """Setup test data for the route test"""
        ready = JOB_STATUS_DICT['ready']
        csv_validation = JOB_TYPE_DICT['csv_record_validation']

        job = insert_job(
            sess,
            filetype=FILE_TYPE_DICT['award'],
            status=ready,
            type_id=csv_validation,
            submission=submission_id
        )
        # everything is fine
        FileTests.insert_file(sess, job.job_id, FILE_STATUS_DICT['complete'])

        job = insert_job(
            sess,
            filetype=FILE_TYPE_DICT['award_financial'],
            status=ready,
            type_id=csv_validation,
            submission=submission_id
        )
        # bad header
        FileTests.insert_file(sess, job.job_id, FILE_STATUS_DICT['unknown_error'])

        job = insert_job(
            sess,
            filetype=FILE_TYPE_DICT['appropriations'],
            status=ready,
            type_id=csv_validation,
            submission=submission_id
        )
        # validation level errors
        FileTests.insert_file(sess, job.job_id, FILE_STATUS_DICT['complete'])
        cls.insert_row_level_error(sess, job.job_id)
