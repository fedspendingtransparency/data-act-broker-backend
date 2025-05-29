from datetime import datetime

from dataactcore.interfaces.db import GlobalDB

from dataactcore.models.jobModels import CertifyHistory, PublishHistory, PublishedFilesHistory
from dataactcore.models.userModel import User
from dataactcore.models.domainModels import CGAC, FREC
from dataactcore.models.lookups import PUBLISH_STATUS_DICT, FILE_TYPE_DICT_LETTER_ID

from dataactvalidator.health_check import create_app

from tests.integration.baseTestAPI import BaseTestAPI
from tests.integration.integration_test_helper import insert_submission


class ListLatestPublishedFileTests(BaseTestAPI):
    """Test file submission routes."""

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources (test data)"""
        super(ListLatestPublishedFileTests, cls).setUpClass()
        # TODO: refactor into a pytest fixture

        with create_app().app_context():
            # get the submission test user
            sess = GlobalDB.db().session
            cls.session = sess

            other_user = sess.query(User).filter(User.email == cls.test_users["agency_user"]).one()
            cls.other_user_email = other_user.email
            cls.other_user_id = other_user.user_id
            cls.submission_user_id = other_user.user_id

            # ======= Reference ======
            cgac = CGAC(cgac_id=11, cgac_code="111", agency_name="CGAC 1")
            frec = FREC(frec_id=12, cgac_id=11, frec_code="2222", agency_name="FREC 2")
            cgac2 = CGAC(cgac_id=13, cgac_code="333", agency_name="CGAC 3")
            sess.add_all([cgac, frec, cgac2])
            sess.commit()

            year = 2020
            period = 6
            diff_year = 2021
            diff_period = 7

            # ======= DABS =======
            cls.dabs_sub_unpub = insert_submission(
                sess,
                cls.submission_user_id,
                cgac_code=cgac2.cgac_code,
                reporting_fiscal_year=1999,
                reporting_fisacal_period=2,
                publish_status_id=PUBLISH_STATUS_DICT["unpublished"],
                is_fabs=False,
            )
            cls.dabs_sub_pub_twice = insert_submission(
                sess,
                cls.submission_user_id,
                cgac_code=cgac.cgac_code,
                reporting_fiscal_year=year,
                reporting_fisacal_period=period,
                publish_status_id=PUBLISH_STATUS_DICT["published"],
                is_fabs=False,
            )
            cls.setup_published_submission(sess, cls.dabs_sub_pub_twice, date="01/01/2020", is_fabs=False)
            cls.setup_published_submission(sess, cls.dabs_sub_pub_twice, date="01/02/2020", is_fabs=False)

            cls.dabs_sub_pub_diff_agency = insert_submission(
                sess,
                cls.submission_user_id,
                frec_code=frec.frec_code,
                reporting_fiscal_year=year,
                reporting_fisacal_period=period,
                publish_status_id=PUBLISH_STATUS_DICT["published"],
                is_fabs=False,
            )
            cls.setup_published_submission(sess, cls.dabs_sub_pub_diff_agency, is_fabs=False)

            cls.dabs_sub_pub_diff_year = insert_submission(
                sess,
                cls.submission_user_id,
                cgac_code=cgac.cgac_code,
                reporting_fiscal_year=diff_year,
                reporting_fisacal_period=period,
                publish_status_id=PUBLISH_STATUS_DICT["published"],
                is_fabs=False,
            )
            cls.setup_published_submission(sess, cls.dabs_sub_pub_diff_year, is_fabs=False)

            cls.dabs_sub_pub_diff_period = insert_submission(
                sess,
                cls.submission_user_id,
                cgac_code=cgac.cgac_code,
                reporting_fiscal_year=year,
                reporting_fisacal_period=diff_period,
                publish_status_id=PUBLISH_STATUS_DICT["published"],
                is_fabs=False,
            )
            cls.setup_published_submission(sess, cls.dabs_sub_pub_diff_period, is_fabs=False)

            # ======= FABS =======
            cls.fabs_sub_unpub = insert_submission(
                sess,
                cls.submission_user_id,
                cgac_code="333",
                reporting_fiscal_year=None,
                reporting_fisacal_period=None,
                publish_status_id=1,
                is_fabs=True,
            )

            cls.fabs_sub_pub = insert_submission(
                sess,
                cls.submission_user_id,
                cgac_code=cgac.cgac_code,
                reporting_fiscal_year=None,
                reporting_fisacal_period=None,
                publish_status_id=PUBLISH_STATUS_DICT["published"],
                is_fabs=True,
            )
            cls.setup_published_submission(sess, cls.fabs_sub_pub, date="10/01/2000", is_fabs=True)
            cls.fabs_sub_pub_2 = insert_submission(
                sess,
                cls.submission_user_id,
                cgac_code=cgac.cgac_code,
                reporting_fiscal_year=None,
                reporting_fisacal_period=None,
                publish_status_id=PUBLISH_STATUS_DICT["published"],
                is_fabs=True,
            )
            cls.setup_published_submission(sess, cls.fabs_sub_pub_2, date="10/02/2000", is_fabs=True)

            cls.fabs_sub_pub_diff_agency = insert_submission(
                sess,
                cls.submission_user_id,
                frec_code=frec.frec_code,
                reporting_fiscal_year=None,
                reporting_fisacal_period=None,
                publish_status_id=PUBLISH_STATUS_DICT["published"],
                is_fabs=True,
            )
            cls.setup_published_submission(sess, cls.fabs_sub_pub_diff_agency, date="10/01/2000", is_fabs=True)

            cls.fabs_sub_pub_diff_year = insert_submission(
                sess,
                cls.submission_user_id,
                cgac_code=cgac.cgac_code,
                reporting_fiscal_year=None,
                reporting_fisacal_period=None,
                publish_status_id=PUBLISH_STATUS_DICT["published"],
                is_fabs=True,
            )
            cls.setup_published_submission(sess, cls.fabs_sub_pub_diff_year, date="10/01/2001", is_fabs=True)

            cls.fabs_sub_pub_diff_period = insert_submission(
                sess,
                cls.submission_user_id,
                cgac_code=cgac.cgac_code,
                reporting_fiscal_year=None,
                reporting_fisacal_period=None,
                publish_status_id=PUBLISH_STATUS_DICT["published"],
                is_fabs=True,
            )
            cls.setup_published_submission(sess, cls.fabs_sub_pub_diff_period, date="01/01/2001", is_fabs=True)

    def setUp(self):
        """Test set-up."""
        super(ListLatestPublishedFileTests, self).setUp()
        self.login_user()

    @classmethod
    def setup_published_submission(cls, sess, submission_id, date="01/01/2000", is_fabs=False):
        ch = CertifyHistory(user_id=cls.submission_user_id, submission_id=submission_id)
        ph = PublishHistory(user_id=cls.submission_user_id, submission_id=submission_id)
        sess.add_all([ch, ph])
        sess.commit()

        file_type_list = ["A", "B", "C", "D1", "D2", "E", "F"] if not is_fabs else ["FABS"]
        for file_type_letter in file_type_list:
            cls.insert_published_files_history(
                sess,
                ch.certify_history_id,
                ph.publish_history_id,
                submission_id,
                date,
                FILE_TYPE_DICT_LETTER_ID[file_type_letter],
                "path/to/file_{}.csv".format(file_type_letter),
                None,
                None,
            )
        if not is_fabs:
            cls.insert_published_files_history(
                sess,
                ch.certify_history_id,
                ph.publish_history_id,
                submission_id,
                date,
                None,
                "path/to/comments.csv",
                None,
                None,
            )

        return ch.certify_history_id, ph.publish_history_id

    @classmethod
    def insert_published_files_history(
        cls,
        sess,
        ch_id,
        ph_id,
        submission_id,
        date="01/01/2000",
        file_type=None,
        filename=None,
        warning_filename=None,
        comment=None,
    ):
        """Insert one history entry into published files history database."""
        cfh = PublishedFilesHistory(
            certify_history_id=ch_id,
            publish_history_id=ph_id,
            submission_id=submission_id,
            filename=filename,
            file_type_id=file_type,
            warning_filename=warning_filename,
            comment=comment,
            created_at=date or datetime.now(),
        )
        sess.add(cfh)
        sess.commit()
        return cfh.published_files_history_id

    def get_response(self, **params):
        return self.app.get(
            "/v1/list_latest_published_files/", headers={"x-session-id": self.session_id}, params=params
        )

    def test_list_latest_published_files_dabs(self):
        # agency
        expected_response = [{"id": "111", "label": "111 - CGAC 1"}, {"id": "2222", "label": "2222 - FREC 2"}]
        response = self.get_response(type="dabs")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, expected_response)

        # year
        expected_response = [{"id": 2020, "label": "2020"}, {"id": 2021, "label": "2021"}]
        response = self.get_response(type="dabs", agency="111")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, expected_response)

        # period
        expected_response = [{"id": 6, "label": "P06/Q2"}, {"id": 7, "label": "P07"}]
        response = self.get_response(type="dabs", agency="111", year="2020")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, expected_response)

        # files
        expected_response = [
            {"id": 9, "label": "file_A.csv", "filetype": "A", "submission_id": self.dabs_sub_pub_twice},
            {"id": 10, "label": "file_B.csv", "filetype": "B", "submission_id": self.dabs_sub_pub_twice},
            {"id": 11, "label": "file_C.csv", "filetype": "C", "submission_id": self.dabs_sub_pub_twice},
            {"id": 13, "label": "file_D2.csv", "filetype": "D2", "submission_id": self.dabs_sub_pub_twice},
            {"id": 12, "label": "file_D1.csv", "filetype": "D1", "submission_id": self.dabs_sub_pub_twice},
            {"id": 14, "label": "file_E.csv", "filetype": "E", "submission_id": self.dabs_sub_pub_twice},
            {"id": 15, "label": "file_F.csv", "filetype": "F", "submission_id": self.dabs_sub_pub_twice},
        ]
        response = self.get_response(type="dabs", agency="111", year="2020", period=6)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, expected_response)

    def test_list_latest_published_files_fabs(self):
        # agency
        expected_response = [{"id": "111", "label": "111 - CGAC 1"}, {"id": "2222", "label": "2222 - FREC 2"}]
        response = self.get_response(type="fabs")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, expected_response)

        # year
        expected_response = [{"id": 2001, "label": "2001"}, {"id": 2002, "label": "2002"}]
        response = self.get_response(type="fabs", agency="111")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, expected_response)

        # period
        expected_response = [{"id": 1, "label": "P01"}, {"id": 4, "label": "P04"}]
        response = self.get_response(type="fabs", agency="111", year="2001")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, expected_response)

        # files
        expected_response = [
            {"id": 41, "label": "file_FABS.csv", "filetype": "FABS", "submission_id": self.fabs_sub_pub},
            {"id": 42, "label": "file_FABS.csv", "filetype": "FABS", "submission_id": self.fabs_sub_pub_2},
        ]
        response = self.get_response(type="fabs", agency="111", year="2001", period="1")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, expected_response)
