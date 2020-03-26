from datetime import datetime

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.userModel import User
from dataactcore.models.lookups import PUBLISH_STATUS_DICT, FILE_TYPE_DICT, FILE_STATUS_DICT, JOB_TYPE_DICT

from dataactvalidator.health_check import create_app

from tests.integration.baseTestAPI import BaseTestAPI
from tests.integration.integration_test_helper import insert_submission, insert_job, get_submission


class ListSubmissionTests(BaseTestAPI):
    """ Test list submissions endpoint """

    MAX_UPDATED_AT = '01/01/3000'

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources (test data)"""
        super(ListSubmissionTests, cls).setUpClass()
        # TODO: refactor into a pytest fixture

        with create_app().app_context():
            # get an admin and non-admin user
            sess = GlobalDB.db().session
            cls.session = sess
            admin_user = sess.query(User).filter(User.email == cls.test_users['admin_user']).one()
            cls.admin_user_id = admin_user.user_id

            other_user = sess.query(User).filter(User.email == cls.test_users['agency_user']).one()
            cls.other_user_id = other_user.user_id

            # set up submissions for dabs
            cls.non_admin_dabs_sub_id = insert_submission(sess, cls.other_user_id, cgac_code='SYS',
                                                          start_date='10/2015', end_date='12/2015', is_quarter=True,
                                                          is_fabs=False,
                                                          publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                                          updated_at='01/01/2010')

            cls.admin_dabs_sub_id = insert_submission(sess, cls.admin_user_id, cgac_code='000', start_date='10/2015',
                                                      end_date='12/2015', is_quarter=True, is_fabs=False,
                                                      publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                                      updated_at='01/01/2012')

            # This is the min date, but the date everything should be using is the one in the job (MAX_UPDATED_AT)
            cls.certified_dabs_sub_id = insert_submission(sess, cls.admin_user_id, cgac_code='SYS',
                                                          start_date='10/2015', end_date='12/2015', is_quarter=True,
                                                          is_fabs=False,
                                                          publish_status_id=PUBLISH_STATUS_DICT['published'],
                                                          updated_at='01/01/2000')

            # Add a couple jobs for dabs files, make sure the updated at is the same as or earlier than the one on
            # the submission itself
            insert_job(sess, FILE_TYPE_DICT['appropriations'], FILE_STATUS_DICT['complete'],
                       JOB_TYPE_DICT['file_upload'], cls.non_admin_dabs_sub_id, filename='/path/to/test/file_1.csv',
                       file_size=123, num_rows=3, updated_at='01/01/2009')
            insert_job(sess, FILE_TYPE_DICT['award'], FILE_STATUS_DICT['complete'], JOB_TYPE_DICT['file_upload'],
                       cls.non_admin_dabs_sub_id, filename='/path/to/test/file_2.csv', file_size=123, num_rows=3,
                       updated_at='01/01/2009')

            # Min updated at date
            insert_job(sess, FILE_TYPE_DICT['award'], FILE_STATUS_DICT['complete'], JOB_TYPE_DICT['file_upload'],
                       cls.certified_dabs_sub_id, filename='/path/to/test/file_part_2.csv', file_size=123, num_rows=3,
                       updated_at=cls.MAX_UPDATED_AT)

            # set up submissions for fabs
            cls.non_admin_fabs_sub_id = insert_submission(sess, cls.admin_user_id, cgac_code='SYS',
                                                          start_date='10/2015', end_date='12/2015', is_fabs=True,
                                                          publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                                          updated_at='01/01/2016')

            # This is the min date, but the date everything should be using is the one in the job (MAX_UPDATED_AT)
            cls.admin_fabs_sub_id = insert_submission(sess, cls.other_user_id, cgac_code='000', start_date='10/2015',
                                                      end_date='12/2015', is_fabs=True,
                                                      publish_status_id=PUBLISH_STATUS_DICT['unpublished'],
                                                      updated_at='01/01/2000')

            cls.published_fabs_sub_id = insert_submission(sess, cls.other_user_id, cgac_code='000',
                                                          start_date='10/2015', end_date='12/2015', is_fabs=True,
                                                          publish_status_id=PUBLISH_STATUS_DICT['published'],
                                                          updated_at='01/02/2000')

            # Add a job for a FABS submission
            insert_job(sess, FILE_TYPE_DICT['fabs'], FILE_STATUS_DICT['complete'], JOB_TYPE_DICT['file_upload'],
                       cls.admin_fabs_sub_id, filename=str(cls.admin_fabs_sub_id) + '/test_file.csv', file_size=123,
                       num_rows=3, updated_at=cls.MAX_UPDATED_AT)

    def setUp(self):
        """ Test set-up. """
        super(ListSubmissionTests, self).setUp()
        self.login_admin_user()

    def sub_ids(self, response):
        """ Helper function to parse out the submission ids from an HTTP response. """
        self.assertEqual(response.status_code, 200)
        result = response.json
        self.assertIn('submissions', result)
        return {sub['submission_id'] for sub in result['submissions']}

    def test_list_submissions_dabs_admin(self):
        """ Test with DABS submissions for an admin user. """
        response = self.app.post_json('/v1/list_submissions/', {'certified': 'mixed'},
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.non_admin_dabs_sub_id, self.admin_dabs_sub_id,
                                                  self.certified_dabs_sub_id})
        self.assertEqual(response.json['min_last_modified'],
                         str(get_submission(self.session, self.non_admin_dabs_sub_id).updated_at))

        response = self.app.post_json('/v1/list_submissions/', {'certified': 'false'},
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.non_admin_dabs_sub_id, self.admin_dabs_sub_id})
        self.assertEqual(response.json['min_last_modified'],
                         str(get_submission(self.session, self.non_admin_dabs_sub_id).updated_at))

        response = self.app.post_json('/v1/list_submissions/', {'certified': 'true'},
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.certified_dabs_sub_id})
        self.assertEqual(response.json['min_last_modified'], str(datetime.strptime(self.MAX_UPDATED_AT, '%m/%d/%Y')))

    def test_list_submissions_dabs_non_admin(self):
        """ Test with DABS submissions for a non admin user. """
        self.login_user()
        response = self.app.post_json('/v1/list_submissions/', {'certified': 'mixed'},
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.non_admin_dabs_sub_id, self.admin_dabs_sub_id})
        self.assertEqual(response.json['min_last_modified'],
                         str(get_submission(self.session, self.non_admin_dabs_sub_id).updated_at))

        response = self.app.post_json('/v1/list_submissions/', {'certified': 'false'},
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.non_admin_dabs_sub_id, self.admin_dabs_sub_id})
        self.assertEqual(response.json['min_last_modified'],
                         str(get_submission(self.session, self.non_admin_dabs_sub_id).updated_at))

        response = self.app.post_json('/v1/list_submissions/', {'certified': 'true'},
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), set())
        self.assertEqual(response.json['min_last_modified'], None)

    def test_list_submissions_fabs_admin(self):
        """ Test with FABS submissions for an admin user. """
        response = self.app.post_json('/v1/list_submissions/', {'certified': 'mixed', 'fabs': True},
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.non_admin_fabs_sub_id, self.admin_fabs_sub_id,
                                                  self.published_fabs_sub_id})
        self.assertEqual(response.json['min_last_modified'],
                         str(get_submission(self.session, self.published_fabs_sub_id).updated_at))

        response = self.app.post_json('/v1/list_submissions/', {'certified': 'false', 'fabs': True},
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.non_admin_fabs_sub_id, self.admin_fabs_sub_id})
        self.assertEqual(response.json['min_last_modified'],
                         str(get_submission(self.session, self.non_admin_fabs_sub_id).updated_at))

        response = self.app.post_json('/v1/list_submissions/', {'certified': 'true', 'fabs': True},
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.published_fabs_sub_id})
        self.assertEqual(response.json['min_last_modified'],
                         str(get_submission(self.session, self.published_fabs_sub_id).updated_at))

    def test_list_submissions_fabs_non_admin(self):
        """ Test with FABS submissions for a non admin user. """
        self.login_user()
        response = self.app.post_json('/v1/list_submissions/', {'certified': 'mixed', 'fabs': True},
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.admin_fabs_sub_id, self.published_fabs_sub_id})
        self.assertEqual(response.json['min_last_modified'],
                         str(get_submission(self.session, self.published_fabs_sub_id).updated_at))

        response = self.app.post_json('/v1/list_submissions/', {'certified': 'false', 'fabs': True},
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.admin_fabs_sub_id})
        self.assertEqual(response.json['min_last_modified'], str(datetime.strptime(self.MAX_UPDATED_AT, '%m/%d/%Y')))

        response = self.app.post_json('/v1/list_submissions/', {'certified': 'true', 'fabs': True},
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.published_fabs_sub_id})
        self.assertEqual(response.json['min_last_modified'],
                         str(get_submission(self.session, self.published_fabs_sub_id).updated_at))

    def test_list_submissions_filter_id(self):
        """ Test listing submissions with a submission_id filter applied. """
        # Listing only the relevant submissions, even when an ID is provided that can't be reached
        post_json = {
            'certified': 'mixed',
            'filters': {
                'submission_ids': [self.non_admin_dabs_sub_id, self.admin_fabs_sub_id]
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.non_admin_dabs_sub_id})

        self.login_user()
        # Not returning a result if the user doesn't have access to the submission
        post_json['filters'] = {
            'submission_ids': [self.certified_dabs_sub_id]
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), set())
        # Proving that filters don't affect min last modified
        self.assertEqual(response.json['min_last_modified'],
                         str(get_submission(self.session, self.non_admin_dabs_sub_id).updated_at))

    def test_list_submissions_filter_date(self):
        """ Test listing submissions with a start and end date filter applied. """
        # Listing only submissions that have been updated in the time frame
        post_json = {
            'certified': 'mixed',
            'filters': {
                'last_modified_range': {
                    'start_date': '12/31/2009',
                    'end_date': '01/30/2010'
                }
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.non_admin_dabs_sub_id})

        # Time frame with no submission updates
        post_json['filters'] = {
            'last_modified_range': {
                'start_date': '12/31/2010',
                'end_date': '01/30/2011'
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), set())

        # One day date range (shows inclusivity)
        post_json['filters'] = {
            'last_modified_range': {
                'start_date': '01/01/2010',
                'end_date': '01/01/2010'
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.non_admin_dabs_sub_id})

        # Listing submissions based on last modified in job (if it's higher) (also still using only one day)
        post_json = {
            'certified': 'mixed',
            'filters': {
                'last_modified_range': {
                    'start_date': self.MAX_UPDATED_AT,
                    'end_date': self.MAX_UPDATED_AT
                }
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.certified_dabs_sub_id})

        # Works if one of the date filters isn't provided and the other is
        post_json['filters'] = {
            'last_modified_range': {
                'start_date': '01/01/2010'
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 200)

        # Breaks if something other than start_date and end_date is passed and neither of the correct ones is
        post_json['filters'] = {
            'last_modified_range': {
                'start': '01/01/2010'
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['message'], 'At least start_date or end_date must be provided when using '
                                                   'last_modified_range filter')

        # Breaks if date isn't valid
        post_json['filters'] = {
            'last_modified_range': {
                'start_date': '30/30/2010',
                'end_date': '01/01/2010'
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Start or end date cannot be parsed into a date of format '
                                                   'MM/DD/YYYY')

        # Breaks if start date is after end date
        post_json['filters'] = {
            'last_modified_range': {
                'start_date': '01/02/2010',
                'end_date': '01/01/2010'
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Last modified start date cannot be greater than the end date')

        # Breaks if last_modified_range isn't an object
        post_json['filters'] = {
            'last_modified_range': [123, 456]
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'last_modified_range filter must be null or an object')

    def test_list_submissions_filter_agency(self):
        """ Test listing submissions with an agency_code filter applied. """
        # Listing only the relevant submissions
        post_json = {
            'certified': 'mixed',
            'filters': {
                'agency_codes': ['000']
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.admin_dabs_sub_id})

        self.login_user()
        # Not returning a result if the user doesn't have access to the submission
        post_json = {
            'certified': 'mixed',
            'fabs': True,
            'filters': {
                'agency_codes': ['SYS']
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), set())

        self.login_admin_user()
        # Invalid agency code, valid length
        post_json = {
            'certified': 'mixed',
            'filters': {
                'agency_codes': ['111']
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'All codes in the agency_codes filter must be valid agency codes')

        # Invalid agency code, wrong length
        post_json['filters'] = {
            'agency_codes': ['12345']
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'All codes in the agency_codes filter must be valid agency codes')

        # Invalid agency code, contains non-string
        post_json['filters'] = {
            'agency_codes': [['123', '456', '789'], 'SYS']
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'All codes in the agency_codes filter must be valid agency codes')

        # Non-array being passed over
        post_json['filters'] = {
            'agency_codes': 'SYS'
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'agency_codes filter must be null or an array')

    def test_list_submissions_filter_filename(self):
        """ Test listing submissions with an file_names filter applied. """
        # List only submissions with job files
        post_json = {
            'certified': 'mixed',
            'filters': {
                'file_names': ['file']
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.non_admin_dabs_sub_id, self.certified_dabs_sub_id})

        # Not returning a result if the string doesn't exist in a file name (even if it exists in a path to it)
        post_json['filters'] = {
            'file_names': ['test']
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), set())

        # Returning both submissions if each has even one job that matches one of the given strings (testing multiple)
        post_json['filters'] = {
            'file_names': ['part', '_1']
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.non_admin_dabs_sub_id, self.certified_dabs_sub_id})

        # Non-array being passed over (error)
        post_json['filters'] = {
            'file_names': 'part'
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id},
                                      expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file_names filter must be null or an array')

        # non-local style submission
        post_json = {
            'certified': 'mixed',
            'fabs': True,
            'filters': {
                'file_names': ['test']
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.admin_fabs_sub_id})

        # Ignores the ID (despite it being part of the file path, but not the name)
        post_json['filters'] = {
            'file_names': [str(self.admin_fabs_sub_id)]
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), set())

    def test_list_submissions_filter_user_id(self):
        """ Test listing submissions with a user_id filter applied. """
        # Listing only the relevant submissions, even when an ID is provided that can't be reached
        post_json = {
            'certified': 'mixed',
            'filters': {
                'user_ids': [self.other_user_id, -1]
            }
        }
        response = self.app.post_json('/v1/list_submissions/', post_json, headers={'x-session-id': self.session_id})
        self.assertEqual(self.sub_ids(response), {self.non_admin_dabs_sub_id})
