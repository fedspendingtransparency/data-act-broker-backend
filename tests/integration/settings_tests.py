from dataactcore.interfaces.db import GlobalDB

from dataactvalidator.health_check import create_app
from dataactcore.models.domainModels import CGAC
from dataactcore.models.validationModels import RuleSetting, RuleSql

from tests.integration.baseTestAPI import BaseTestAPI


class SettingsTests(BaseTestAPI):
    """ Test settings routes. """

    @classmethod
    def setUpClass(cls):
        """ Set up class-wide resources (test data) """
        super(SettingsTests, cls).setUpClass()
        # TODO: refactor into a pytest fixture

        with create_app().app_context():
            # get the submission test user
            sess = GlobalDB.db().session
            cls.session = sess

            cgac = CGAC(cgac_code='097')
            rule = RuleSql(rule_sql_id=1, rule_sql='', rule_label='FABS1', rule_error_message='', query_name='',
                           file_id=1, rule_severity_id=2, rule_cross_file_flag=False)
            sess.add_all([cgac, rule])
            sess.commit()
            default_setting = RuleSetting(agency_code='097', rule_id=rule.rule_sql_id, priority=1, impact_id=1)
            sess.add(default_setting)
            sess.commit()

    def setUp(self):
        """ Test set-up. """
        super(SettingsTests, self).setUp()
        self.login_admin_user()

    def test_get_rule_settings(self):
        """ Test successfully getting the agency rules settings """
        # Basic passing test
        rule_settings_params = {'agency_code': '097', 'file': 'B'}
        response = self.app.get('/v1/rule_settings/', rule_settings_params, headers={'x-session-id': self.session_id})

        self.assertEqual(response.status_code, 200)
        assert {'errors', 'warnings'} <= set(response.json.keys())

    def test_get_rule_settings_fail(self):
        """ Test failing getting the agency rules settings """
        # Not including any required filters
        rule_settings_params = {}
        response = self.app.get('/v1/rule_settings/', rule_settings_params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'agency_code: Missing data for required field.'
                                                   ' file: Missing data for required field.')

        # Not including some required filters
        rule_settings_params = {'agency_code': ''}
        response = self.app.get('/v1/rule_settings/', rule_settings_params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file: Missing data for required field.')

        # Not including some required filters
        rule_settings_params = {'agency_code': '', 'file': ''}
        response = self.app.get('/v1/rule_settings/', rule_settings_params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file: Must be A, B, C, cross-AB, cross-BC, cross-CD1, or cross-CD2')

        # Not including some required filters
        rule_settings_params = {'agency_code': '', 'file': 'cross-D1'}
        response = self.app.get('/v1/rule_settings/', rule_settings_params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file: Must be A, B, C, cross-AB, cross-BC, cross-CD1, or cross-CD2')

        # Wrong agency code
        rule_settings_params = {'agency_code': 'BAD', 'file': 'C'}
        response = self.app.get('/v1/rule_settings/', rule_settings_params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Invalid agency_code: BAD')

        # Wrong file
        rule_settings_params = {'agency_code': '097', 'file': 'BAD'}
        response = self.app.get('/v1/rule_settings/', rule_settings_params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file: Must be A, B, C, cross-AB, cross-BC, cross-CD1, or cross-CD2')

    def test_save_rule_settings(self):
        """ Test successfully saving the agency rule settings """
        # Basic passing test
        rule_settings_params = {'agency_code': '097', 'rules': []}
        response = self.app.get('/v1/save_rule_settings/', rule_settings_params, headers={'x-session-id': self.session_id})

        self.assertEqual(response.status_code, 200)
        assert response.json == {"message": "Agency 097 rules saved."}

    def test_save_rule_settings_fail(self):
        """ Test failing saving the agency rule settings """
        # Not including any required filters
        rule_settings_params = {}
        response = self.app.get('/v1/rule_settings/', rule_settings_params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'agency_code: Missing data for required field.'
                                                   ' file: Missing data for required field.')

        # Not including some required filters
        rule_settings_params = {'agency_code': ''}
        response = self.app.get('/v1/rule_settings/', rule_settings_params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file: Missing data for required field.')

        # Not including some required filters
        rule_settings_params = {'agency_code': '', 'file': ''}
        response = self.app.get('/v1/rule_settings/', rule_settings_params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file: Must be A, B, C, cross-AB, cross-BC, cross-CD1, or cross-CD2')

        # Not including some required filters
        rule_settings_params = {'agency_code': '', 'file': 'cross-D1'}
        response = self.app.get('/v1/rule_settings/', rule_settings_params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file: Must be A, B, C, cross-AB, cross-BC, cross-CD1, or cross-CD2')

        # Wrong agency code
        rule_settings_params = {'agency_code': 'BAD', 'file': 'C'}
        response = self.app.get('/v1/rule_settings/', rule_settings_params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Invalid agency_code: BAD')

        # Wrong file
        rule_settings_params = {'agency_code': '097', 'file': 'BAD'}
        response = self.app.get('/v1/rule_settings/', rule_settings_params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file: Must be A, B, C, cross-AB, cross-BC, cross-CD1, or cross-CD2')
