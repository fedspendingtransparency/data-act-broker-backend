from dataactcore.utils.statusCode import StatusCode

class TestStatusCodes:

    def setup_class(self):
        """ Setup for class """

    def teardown_class(self):
        """ Teardown for class """

    def test_status_ok(self):
        assert StatusCode.OK == 200

    def test_status_client_error(self):
        assert StatusCode.CLIENT_ERROR == 400

    def test_status_login_required(self):
        assert StatusCode.LOGIN_REQUIRED == 401

    def test_status_internal_error(self):
        assert StatusCode.INTERNAL_ERROR == 500

    def test_intentional_failure(self):
        assert StatusCode.OK == 201