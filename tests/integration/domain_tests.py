from tests.integration.baseTestAPI import BaseTestAPI
from dataactbroker.app import create_app
from dataactcore.interfaces.db import GlobalDB
from tests.unit.dataactcore.factories.domain import CGACFactory, FRECFactory
from dataactcore.models.userModel import User, UserAffiliation
from dataactcore.utils.statusCode import StatusCode
from dataactcore.models.lookups import PERMISSION_SHORT_DICT


class DomainTests(BaseTestAPI):
    """ Test domain specific functions """

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources like users."""
        super(DomainTests, cls).setUpClass()

        with create_app().app_context():
            sess = GlobalDB.db().session

            user = User()
            r_cgac = CGACFactory()
            w_cgac = CGACFactory()
            s_frec_cgac = CGACFactory()
            s_frec = FRECFactory(cgac=s_frec_cgac)
            e_frec_cgac = CGACFactory()
            e_frec = FRECFactory(cgac=e_frec_cgac)
            f_cgac = CGACFactory()
            user.affiliations = [UserAffiliation(cgac=r_cgac, frec=None, permission_type_id=PERMISSION_SHORT_DICT['r']),
                                 UserAffiliation(cgac=w_cgac, frec=None, permission_type_id=PERMISSION_SHORT_DICT['w']),
                                 UserAffiliation(cgac=None, frec=s_frec, permission_type_id=PERMISSION_SHORT_DICT['s']),
                                 UserAffiliation(cgac=None, frec=e_frec, permission_type_id=PERMISSION_SHORT_DICT['e']),
                                 UserAffiliation(cgac=f_cgac, frec=None, permission_type_id=PERMISSION_SHORT_DICT['f'])]
            sess.add_all([r_cgac, w_cgac, s_frec_cgac, s_frec, e_frec_cgac, e_frec, f_cgac])
            sess.commit()

    def setUp(self):
        """Test set-up."""
        super(DomainTests, self).setUp()
        self.login_user()

    def test_list_agencies_success(self):
        """Test retrieving list agencies information."""
        response = self.app.get("/v1/list_agencies/", headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK)
        self.assertEqual(response.status_code, 200)
        assert {'cgac_agency_list', 'frec_agency_list'} <= set(response.json.keys())

        query_params = {'perm_level': 'writer', 'perm_type': 'dabs'}
        response = self.app.get("/v1/list_agencies/", query_params, headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK)
        self.assertEqual(response.status_code, 200)
        assert {'cgac_agency_list', 'frec_agency_list'} == set(response.json.keys())

    def test_list_agencies_fail(self):
        """Test failing retrieving list agencies information."""
        query_params = {'perm_level': 'writer', 'perm_type': 'test'}
        response = self.app.get("/v1/list_agencies/", query_params, headers={"x-session-id": self.session_id},
                                expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'perm_type: Not a valid choice.')

        query_params = {'perm_level': 'test', 'perm_type': 'fabs'}
        response = self.app.get("/v1/list_agencies/", query_params, headers={"x-session-id": self.session_id},
                                expect_errors=True)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'perm_level: Not a valid choice.')

    def test_list_all_agencies_success(self):
        """Test retrieving list agencies information."""
        response = self.app.get("/v1/list_all_agencies/", headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK)
        self.assertEqual(response.status_code, 200)
        assert {'agency_list', 'shared_agency_list'} == set(response.json.keys())

    def test_list_sub_tier_agencies_success(self):
        """Test retrieving list agencies information."""
        response = self.app.get("/v1/list_sub_tier_agencies/", headers={"x-session-id": self.session_id})
        self.check_response(response, StatusCode.OK)
        self.assertEqual(response.status_code, 200)
        assert {'sub_tier_agency_list'} == set(response.json.keys())
