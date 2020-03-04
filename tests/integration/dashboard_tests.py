from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.userModel import User

from dataactvalidator.health_check import create_app

from tests.integration.baseTestAPI import BaseTestAPI
from tests.integration.integration_test_helper import insert_submission


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

            cls.quarter_sub = insert_submission(cls.session, cls.submission_user_id, cgac_code='SYS',
                                                start_date='01/2017', end_date='03/2017', is_quarter=True)

    def setUp(self):
        """ Test set-up. """
        super(DashboardTests, self).setUp()
        self.login_admin_user()

    def test_post_dabs_summary(self):
        """ Test successfully getting the dabs summary """
        # Basic passing test
        dabs_summary_json = {'filters': {'quarters': [], 'fys': [], 'agencies': []}}
        response = self.app.post_json('/v1/historic_dabs_summary/', dabs_summary_json,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertEqual([{'agency_name': 'Admin Agency', 'submissions': []},
                          {'agency_name': 'Example Agency', 'submissions': []}],
                         sorted(response.json, key=lambda agency: agency['agency_name']))

    def test_post_dabs_summary_fail(self):
        """ Test failing getting the dabs summary """
        # Not including any required filters
        dabs_summary_json = {'filters': {}}
        response = self.app.post_json('/v1/historic_dabs_summary/', dabs_summary_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'The following filters were not provided: quarters, fys, agencies')

        # Not including some required filters
        dabs_summary_json = {'filters': {'quarters': []}}
        response = self.app.post_json('/v1/historic_dabs_summary/', dabs_summary_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'The following filters were not provided: fys, agencies')

        # Wrong quarter
        dabs_summary_json = {'filters': {'quarters': [6], 'fys': [], 'agencies': []}}
        response = self.app.post_json('/v1/historic_dabs_summary/', dabs_summary_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Quarters must be a list of integers, each ranging 1-4,'
                                                   ' or an empty list.')

        # Wrong fys
        dabs_summary_json = {'filters': {'quarters': [], 'fys': [2011], 'agencies': []}}
        response = self.app.post_json('/v1/historic_dabs_summary/', dabs_summary_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Fiscal Years must be a list of integers, each ranging from 2017'
                                                   ' through the current fiscal year, or an empty list.')

        # Wrong agencies - integer instead of a string
        dabs_summary_json = {'filters': {'quarters': [], 'fys': [], 'agencies': [90]}}
        response = self.app.post_json('/v1/historic_dabs_summary/', dabs_summary_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Agencies must be a list of strings, or an empty list.')

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

        # Getting the labels while logged out
        self.logout()
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

    def test_post_dabs_graphs(self):
        """ Test successfully getting the dabs graphs """
        # Basic passing test
        dabs_summary_json = {'filters': {'quarters': [], 'fys': [], 'agencies': [], 'files': [], 'rules': []}}
        response = self.app.post_json('/v1/historic_dabs_graphs/', dabs_summary_json,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertEqual({'A': [], 'B': [], 'C': [], 'cross-AB': [], 'cross-BC': [], 'cross-CD1': [],
                          'cross-CD2': []}, response.json)

    def test_post_dabs_graphs_fail(self):
        """ Test failing getting the dabs graphs """
        # Not including any required filters
        dabs_graphs_json = {'filters': {}}
        response = self.app.post_json('/v1/historic_dabs_graphs/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'The following filters were not provided: quarters, fys, agencies,'
                                                   ' files, rules')

        # Not including some required filters
        dabs_graphs_json = {'filters': {'quarters': [], 'fys': [], 'agencies': []}}
        response = self.app.post_json('/v1/historic_dabs_graphs/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'The following filters were not provided: files, rules')

        # Wrong quarter
        dabs_graphs_json = {'filters': {'quarters': [6], 'fys': [], 'agencies': [], 'files': [], 'rules': []}}
        response = self.app.post_json('/v1/historic_dabs_graphs/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Quarters must be a list of integers, each ranging 1-4,'
                                                   ' or an empty list.')

        # Wrong fys
        dabs_graphs_json = {'filters': {'quarters': [], 'fys': [2011], 'agencies': [], 'files': [], 'rules': []}}
        response = self.app.post_json('/v1/historic_dabs_graphs/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Fiscal Years must be a list of integers, each ranging from 2017'
                                                   ' through the current fiscal year, or an empty list.')

        # Wrong agencies - integer instead of a string
        dabs_graphs_json = {'filters': {'quarters': [], 'fys': [], 'agencies': [90], 'files': [], 'rules': []}}
        response = self.app.post_json('/v1/historic_dabs_graphs/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Agencies must be a list of strings, or an empty list.')

        # Wrong agencies - non-existent agency
        dabs_graphs_json = {'filters': {'quarters': [], 'fys': [], 'agencies': ['999'], 'files': [], 'rules': []}}
        response = self.app.post_json('/v1/historic_dabs_graphs/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'All codes in the agency_codes filter must be valid agency codes')

        # Wrong files
        dabs_graphs_json = {'filters': {'quarters': [], 'fys': [], 'agencies': [], 'files': ['cross-AC'], 'rules': []}}
        response = self.app.post_json('/v1/historic_dabs_graphs/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Files must be a list of one or more of the following,'
                                                   ' or an empty list: A, B, C, cross-AB, cross-BC, cross-CD1,'
                                                   ' cross-CD2')

        # Wrong rules
        dabs_graphs_json = {'filters': {'quarters': [], 'fys': [], 'agencies': [], 'files': [], 'rules': [9]}}
        response = self.app.post_json('/v1/historic_dabs_graphs/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Rules must be a list of strings, or an empty list.')

    def test_historic_dabs_table(self):
        """ Test successfully getting the historic dabs table """
        # Basic passing test
        dabs_summary_json = {'filters': {'quarters': [], 'fys': [], 'agencies': [], 'files': [], 'rules': []},
                             'page': 1, 'limit': 10, 'sort': 'rule_label', 'order': 'asc'}
        response = self.app.post_json('/v1/historic_dabs_table/', dabs_summary_json,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertEqual({'results': [], 'page_metadata': {'total': 0, 'page': 1, 'limit': 10}}, response.json)

        # Test with none of the optional content
        dabs_summary_json = {'filters': {'quarters': [], 'fys': [], 'agencies': [], 'files': [], 'rules': []}}
        response = self.app.post_json('/v1/historic_dabs_table/', dabs_summary_json,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)
        self.assertEqual({'results': [], 'page_metadata': {'total': 0, 'page': 1, 'limit': 5}}, response.json)

    def test_historic_dabs_table_fail(self):
        """ Test failing getting the historic dabs table """
        # Not including any required filters
        dabs_graphs_json = {'filters': {}}
        response = self.app.post_json('/v1/historic_dabs_table/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'The following filters were not provided: quarters, fys, agencies,'
                                                   ' files, rules')

        # Not including some required filters
        dabs_graphs_json = {'filters': {'quarters': [], 'fys': [], 'agencies': []}}
        response = self.app.post_json('/v1/historic_dabs_table/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'The following filters were not provided: files, rules')

        # Wrong quarter
        dabs_graphs_json = {'filters': {'quarters': [6], 'fys': [], 'agencies': [], 'files': [], 'rules': []}}
        response = self.app.post_json('/v1/historic_dabs_table/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Quarters must be a list of integers, each ranging 1-4,'
                                                   ' or an empty list.')

        # Wrong fys
        dabs_graphs_json = {'filters': {'quarters': [], 'fys': [2011], 'agencies': [], 'files': [], 'rules': []}}
        response = self.app.post_json('/v1/historic_dabs_table/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Fiscal Years must be a list of integers, each ranging from 2017'
                                                   ' through the current fiscal year, or an empty list.')

        # Wrong agencies - integer instead of a string
        dabs_graphs_json = {'filters': {'quarters': [], 'fys': [], 'agencies': [90], 'files': [], 'rules': []}}
        response = self.app.post_json('/v1/historic_dabs_table/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Agencies must be a list of strings, or an empty list.')

        # Wrong agencies - non-existent agency
        dabs_graphs_json = {'filters': {'quarters': [], 'fys': [], 'agencies': ['999'], 'files': [], 'rules': []}}
        response = self.app.post_json('/v1/historic_dabs_table/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'All codes in the agency_codes filter must be valid agency codes')

        # Wrong files
        dabs_graphs_json = {'filters': {'quarters': [], 'fys': [], 'agencies': [], 'files': ['cross-AC'], 'rules': []}}
        response = self.app.post_json('/v1/historic_dabs_table/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Files must be a list of one or more of the following,'
                                                   ' or an empty list: A, B, C, cross-AB, cross-BC, cross-CD1,'
                                                   ' cross-CD2')

        # Wrong rules
        dabs_graphs_json = {'filters': {'quarters': [], 'fys': [], 'agencies': [], 'files': [], 'rules': [9]}}
        response = self.app.post_json('/v1/historic_dabs_table/', dabs_graphs_json, expect_errors=True,
                                      headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Rules must be a list of strings, or an empty list.')

    def test_active_submission_overview(self):
        """ Test successfully getting the active submission overview """
        # Error type not specified
        params = {'submission_id': self.quarter_sub, 'file': 'A'}
        response = self.app.get('/v1/active_submission_overview/', params, headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)

        # Error type specified
        params = {'submission_id': self.quarter_sub, 'file': 'A', 'error_level': 'mixed'}
        response = self.app.get('/v1/active_submission_overview/', params, headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)

    def test_active_submission_overview_fail(self):
        """ Test failing to get the active submission overview """
        # Invalid submission ID
        params = {'submission_id': -1, 'file': 'A'}
        response = self.app.get('/v1/active_submission_overview/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'No such submission')

        # Invalid file type
        params = {'submission_id': self.quarter_sub, 'file': 'Q'}
        response = self.app.get('/v1/active_submission_overview/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file: Must be A, B, C, cross-AB, cross-BC, cross-CD1, or cross-CD2')

        # Invalid error level
        params = {'submission_id': self.quarter_sub, 'file': 'A', 'error_level': 'all'}
        response = self.app.get('/v1/active_submission_overview/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'error_level: Must be either warning, error, or mixed')

        # Missing submission ID
        params = {'file': 'A'}
        response = self.app.get('/v1/active_submission_overview/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'submission_id is required')

        # Missing file
        params = {'submission_id': self.quarter_sub}
        response = self.app.get('/v1/active_submission_overview/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file: Missing data for required field.')

    def test_get_impact_counts(self):
        """ Test successfully getting the impact counts for a submission """
        # Error type not specified
        params = {'submission_id': self.quarter_sub, 'file': 'A'}
        response = self.app.get('/v1/get_impact_counts/', params, headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)

        # Error type specified
        params = {'submission_id': self.quarter_sub, 'file': 'A', 'error_level': 'mixed'}
        response = self.app.get('/v1/get_impact_counts/', params, headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)

    def test_get_impact_counts_fail(self):
        """ Test failing to get impact counts for a submission """
        # Invalid submission ID
        params = {'submission_id': -1, 'file': 'A'}
        response = self.app.get('/v1/get_impact_counts/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'No such submission')

        # Invalid file type
        params = {'submission_id': self.quarter_sub, 'file': 'Q'}
        response = self.app.get('/v1/get_impact_counts/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file: Must be A, B, C, cross-AB, cross-BC, cross-CD1, or cross-CD2')

        # Invalid error level
        params = {'submission_id': self.quarter_sub, 'file': 'A', 'error_level': 'all'}
        response = self.app.get('/v1/get_impact_counts/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'error_level: Must be either warning, error, or mixed')

        # Missing submission ID
        params = {'file': 'A'}
        response = self.app.get('/v1/get_impact_counts/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'submission_id is required')

        # Missing file
        params = {'submission_id': self.quarter_sub}
        response = self.app.get('/v1/get_impact_counts/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file: Missing data for required field.')

    def test_get_significance_counts(self):
        """ Test successfully getting the significance counts for a submission """
        # Error type not specified
        params = {'submission_id': self.quarter_sub, 'file': 'A'}
        response = self.app.get('/v1/get_significance_counts/', params, headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)

        # Error type specified
        params = {'submission_id': self.quarter_sub, 'file': 'A', 'error_level': 'mixed'}
        response = self.app.get('/v1/get_significance_counts/', params, headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 200)

    def test_get_significance_counts_fail(self):
        """ Test failing to get significance counts for a submission """
        # Invalid submission ID
        params = {'submission_id': -1, 'file': 'A'}
        response = self.app.get('/v1/get_significance_counts/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'No such submission')

        # Invalid file type
        params = {'submission_id': self.quarter_sub, 'file': 'Q'}
        response = self.app.get('/v1/get_significance_counts/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file: Must be A, B, C, cross-AB, cross-BC, cross-CD1, or cross-CD2')

        # Invalid error level
        params = {'submission_id': self.quarter_sub, 'file': 'A', 'error_level': 'all'}
        response = self.app.get('/v1/get_significance_counts/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'error_level: Must be either warning, error, or mixed')

        # Missing submission ID
        params = {'file': 'A'}
        response = self.app.get('/v1/get_significance_counts/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'submission_id is required')

        # Missing file
        params = {'submission_id': self.quarter_sub}
        response = self.app.get('/v1/get_significance_counts/', params, expect_errors=True,
                                headers={'x-session-id': self.session_id})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'file: Missing data for required field.')
