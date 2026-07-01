from tests.integration.baseTestAPI import BaseTestAPI

from dataactcore.utils.statusCode import StatusCode

SPECIFIC_ERROR_MSG = "Specfiic error!"
GENERAL_ERROR_MSG = "An unexpected error occurred."


def setup_broken_endpoint(app):
    @app.route("/endpoint")
    def handle():
        raise Exception(SPECIFIC_ERROR_MSG)


class BaseAPITestsDebugOn(BaseTestAPI):
    def setUp(self):
        super(BaseAPITestsDebugOn, self).setUp(debug=True)
        setup_broken_endpoint(self.app.app)

    def test_error_debug_on(self):
        response = self.app.get("/endpoint", headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, StatusCode.INTERNAL_ERROR)
        self.assertEqual(response.json["message"], SPECIFIC_ERROR_MSG)


class BaseAPITestsDebugOff(BaseTestAPI):
    def setUp(self):
        super(BaseAPITestsDebugOff, self).setUp(debug=False)
        setup_broken_endpoint(self.app.app)

    def test_error_debug_off(self):
        response = self.app.get("/endpoint", headers={"x-session-id": self.session_id}, expect_errors=True)
        self.assertEqual(response.status_code, StatusCode.INTERNAL_ERROR)
        self.assertEqual(response.json["message"], GENERAL_ERROR_MSG)
