from unittest.mock import Mock

from dataactbroker.handlers.aws.sesEmail import sesEmail


def pytest_runtest_setup(item):
    """Big-ol hack. For the sake of our integration tests, mock out sesEmail.
    Ideally, we can be pin-point our mocks in the future"""
    sesEmail.send = Mock()
