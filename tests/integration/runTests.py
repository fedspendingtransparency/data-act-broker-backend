"""Transition module. We need to update our CI to use
py.test tests/integration

rather than

py.test tests/integration/runTests.py
"""
from tests.integration.loginTests import LoginTests             # noqa
from tests.integration.fileTests import FileTests               # noqa
from tests.integration.userTests import UserTests               # noqa
from tests.integration.jobTests import JobTests                 # noqa
from tests.integration.validatorTests import ValidatorTests     # noqa
from tests.integration.fileTypeTests import FileTypeTests       # noqa
from tests.integration.mixedFileTests import MixedFileTests     # noqa


if __name__ == '__main__':
    print("Did you mean to run")
    print("py.test tests/integration/runTests.py")
