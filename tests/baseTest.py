import unittest

from testUtils import TestUtils
class BaseTest(unittest.TestCase):
    """ Test login, logout, and session handling """
    def addUtils(self,utils):
        self.utils = utils

    def setup(self):
        try:
            self.utils
        except:
            self.utils = TestUtils()
