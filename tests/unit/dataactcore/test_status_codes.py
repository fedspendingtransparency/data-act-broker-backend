from dataactcore.utils.statusCode import StatusCode


def test_status_ok():
    assert StatusCode.OK == 200


def test_status_client_error():
    assert StatusCode.CLIENT_ERROR == 400


def test_status_login_required():
    assert StatusCode.LOGIN_REQUIRED == 401


def test_status_permission_denied():
    assert StatusCode.PERMISSION_DENIED == 403


def test_status_not_found():
    assert StatusCode.NOT_FOUND == 404


def test_status_internal_error():
    assert StatusCode.INTERNAL_ERROR == 500
