from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.userModel import User

from dataactvalidator.health_check import create_app

from tests.integration.baseTestAPI import BaseTestAPI


class DashboardTests(BaseTestAPI):
    """ Test dashboard routes. """

    @classmethod
    def setUpClass(cls):
        """ Set up class-wide resources (test data) """
        super(DashboardTests, cls).setUpClass()
        # TODO: refactor into a pytest fixture

        with create_app().app_context():
            # get the submission test user
            sess = GlobalDB.db().session
            cls.session = sess
            submission_user = sess.query(User).filter(User.email == cls.test_users['admin_user']).one()
            cls.submission_user_id = submission_user.user_id

            other_user = sess.query(User).filter(User.email == cls.test_users['agency_user']).one()
            cls.other_user_id = other_user.user_id

            no_submissions_user = sess.query(User).filter(User.email == cls.test_users['no_permissions_user']).one()
            cls.no_submissions_user_email = no_submissions_user.email
            cls.no_submissions_user_id = no_submissions_user.user_id

    def setUp(self):
        """ Test set-up. """
        super(DashboardTests, self).setUp()
        self.login_admin_user()

    def test_get_rule_labels(self):
        """ Test successfully getting a list of rule labels. """
        # Getting all FABS warnings
        rule_label_json = {'files': [], 'fabs': True, 'error_level': 'warning'}
        response = self.app.post_json('/v1/get_rule_labels/', rule_label_json,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertIn('labels', response.json)

        # Getting all DABS errors
        params = {'files': [], 'fabs': False, 'error_level': 'error'}
        response = self.app.post_json('/v1/get_rule_labels/', params, headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertIn('labels', response.json)

        # Leaving all non-required params out
        params = {'files': []}
        response = self.app.post_json('/v1/get_rule_labels/', params, headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertIn('labels', response.json)

        # Specifying a few DABS files with mixed results
        params = {'files': ['C', 'cross-AB'], 'error_level': 'mixed'}
        response = self.app.post_json('/v1/get_rule_labels/', params, headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertIn('labels', response.json)

        # Getting the labels with no permissions
        self.logout()
        self.login_user(username=self.no_submissions_user_email)
        params = {'files': [], 'fabs': True, 'error_level': 'warning'}
        response = self.app.post_json('/v1/get_rule_labels/', params, headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertIn('labels', response.json)

    def test_get_rule_labels_fail(self):
        """ Test failing to get a list of rule labels. """
        # Invalid error level
        params = {'files': [], 'error_level': 'bad'}
        response = self.app.post_json('/v1/get_rule_labels/', params, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'error_level: Must be either warning, error, or mixed')

        # Missing files param
        params = {'error_level': 'warning'}
        response = self.app.post_json('/v1/get_rule_labels/', params, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'files: Missing data for required field.')
