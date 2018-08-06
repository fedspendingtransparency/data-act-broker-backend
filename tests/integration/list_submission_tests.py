from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.userModel import User
from dataactcore.models.lookups import PUBLISH_STATUS_DICT

from dataactvalidator.health_check import create_app

from tests.integration.baseTestAPI import BaseTestAPI
from tests.integration.integration_test_helper import insert_submission


class ListSubmissionTests(BaseTestAPI):
    """ Test list submissions endpoint """

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
            cls.non_admin_dabs_sub_id = insert_submission(sess, cls.other_user_id, cgac_code="SYS",
                                                          start_date="10/2015", end_date="12/2015", is_quarter=True,
                                                          is_fabs=False,
                                                          publish_status_id=PUBLISH_STATUS_DICT['unpublished'])

            cls.admin_dabs_sub_id = insert_submission(sess, cls.admin_user_id, cgac_code="000", start_date="10/2015",
                                                      end_date="12/2015", is_quarter=True, is_fabs=False,
                                                      publish_status_id=PUBLISH_STATUS_DICT['unpublished'])

            cls.certified_dabs_sub_id = insert_submission(sess, cls.admin_user_id, cgac_code="SYS",
                                                          start_date="10/2015", end_date="12/2015", is_quarter=True,
                                                          is_fabs=False,
                                                          publish_status_id=PUBLISH_STATUS_DICT['published'])

            # set up submissions for dabs
            cls.non_admin_fabs_sub_id = insert_submission(sess, cls.admin_user_id, cgac_code="SYS",
                                                          start_date="10/2015", end_date="12/2015", is_fabs=True,
                                                          publish_status_id=PUBLISH_STATUS_DICT['unpublished'])

            cls.admin_fabs_sub_id = insert_submission(sess, cls.other_user_id, cgac_code="000", start_date="10/2015",
                                                      end_date="12/2015", is_fabs=True,
                                                      publish_status_id=PUBLISH_STATUS_DICT['unpublished'])

            cls.published_fabs_sub_id = insert_submission(sess, cls.other_user_id, cgac_code="000",
                                                          start_date="10/2015", end_date="12/2015", is_fabs=True,
                                                          publish_status_id=PUBLISH_STATUS_DICT['published'])

    def setUp(self):
        """ Test set-up. """
        super(ListSubmissionTests, self).setUp()
        self.login_admin_user()

    @staticmethod
    def sub_ids(response):
        """ Helper function to parse out the submission ids from an HTTP response. """
        assert response.status_code == 200
        result = response.json
        assert 'submissions' in result
        return {sub['submission_id'] for sub in result['submissions']}

    def test_list_submissions_dabs_admin(self):
        """ Test with DABS submissions for an admin user. """
        response = self.app.post_json("/v1/list_submissions/", {"certified": "mixed"},
                                      headers={"x-session-id": self.session_id})
        assert self.sub_ids(response) == {self.non_admin_dabs_sub_id, self.admin_dabs_sub_id,
                                          self.certified_dabs_sub_id}

        response = self.app.post_json("/v1/list_submissions/", {"certified": "false"},
                                      headers={"x-session-id": self.session_id})
        assert self.sub_ids(response) == {self.non_admin_dabs_sub_id, self.admin_dabs_sub_id}

        response = self.app.post_json("/v1/list_submissions/", {"certified": "true"},
                                      headers={"x-session-id": self.session_id})
        assert self.sub_ids(response) == {self.certified_dabs_sub_id}

    def test_list_submissions_dabs_non_admin(self):
        """ Test with DABS submissions for a non admin user. """
        self.login_user()
        response = self.app.post_json("/v1/list_submissions/", {"certified": "mixed"},
                                      headers={"x-session-id": self.session_id})
        assert self.sub_ids(response) == {self.non_admin_dabs_sub_id, self.admin_dabs_sub_id}

        response = self.app.post_json("/v1/list_submissions/", {"certified": "false"},
                                      headers={"x-session-id": self.session_id})
        assert self.sub_ids(response) == {self.non_admin_dabs_sub_id, self.admin_dabs_sub_id}

        response = self.app.post_json("/v1/list_submissions/", {"certified": "true"},
                                      headers={"x-session-id": self.session_id})
        assert self.sub_ids(response) == set()

    def test_list_submissions_fabs_admin(self):
        """ Test with FABS submissions for an admin user. """
        response = self.app.post_json("/v1/list_submissions/", {"certified": "mixed", "d2_submission": True},
                                      headers={"x-session-id": self.session_id})
        assert self.sub_ids(response) == {self.non_admin_fabs_sub_id, self.admin_fabs_sub_id,
                                          self.published_fabs_sub_id}

        response = self.app.post_json("/v1/list_submissions/", {"certified": "false", "d2_submission": True},
                                      headers={"x-session-id": self.session_id})
        assert self.sub_ids(response) == {self.non_admin_fabs_sub_id, self.admin_fabs_sub_id}

        response = self.app.post_json("/v1/list_submissions/", {"certified": "true", "d2_submission": True},
                                      headers={"x-session-id": self.session_id})
        assert self.sub_ids(response) == {self.published_fabs_sub_id}

    def test_list_submissions_fabs_non_admin(self):
        """ Test with FABS submissions for a non admin user. """
        self.login_user()
        response = self.app.post_json("/v1/list_submissions/", {"certified": "mixed", "d2_submission": True},
                                      headers={"x-session-id": self.session_id})
        assert self.sub_ids(response) == {self.admin_fabs_sub_id, self.published_fabs_sub_id}

        response = self.app.post_json("/v1/list_submissions/", {"certified": "false", "d2_submission": True},
                                      headers={"x-session-id": self.session_id})
        assert self.sub_ids(response) == {self.admin_fabs_sub_id}

        response = self.app.post_json("/v1/list_submissions/", {"certified": "true", "d2_submission": True},
                                      headers={"x-session-id": self.session_id})
        assert self.sub_ids(response) == {self.published_fabs_sub_id}

    def test_list_submissions_filter_id(self):
        """ Test listing submissions with a submission_id filter applied. """
        # Listing only the relevant submissions, even when an ID is provided that can't be reached
        post_json = {
            "certified": "mixed",
            "filters": {
                "submission_ids": [self.non_admin_dabs_sub_id, self.admin_fabs_sub_id]
            }
        }
        response = self.app.post_json("/v1/list_submissions/", post_json, headers={"x-session-id": self.session_id})
        assert self.sub_ids(response) == {self.non_admin_dabs_sub_id}

        self.login_user()
        # Not returning a result if the user doesn't have access to the submission
        post_json = {
            "certified": "mixed",
            "filters": {
                "submission_ids": [self.certified_dabs_sub_id]
            }
        }
        response = self.app.post_json("/v1/list_submissions/", post_json, headers={"x-session-id": self.session_id})
        assert self.sub_ids(response) == set()
