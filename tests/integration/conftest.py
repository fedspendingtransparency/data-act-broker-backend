from unittest.mock import Mock

from dataactbroker.handlers.aws.sesEmail import SesEmail


def pytest_runtest_setup(item):
    """Big-ol hack. For the sake of our integration tests, mock out SesEmail.
    Ideally, we can be pin-point our mocks in the future"""
    SesEmail.send = Mock()
