import unittest
import inspect
from dataactcore.models.baseInterface import BaseInterface
from dataactbroker.handlers.interfaceHolder import InterfaceHolder
from testUtils import TestUtils
from loginTests import LoginTests
from fileTests import FileTests
from userTests import UserTests

BaseInterface.IS_FLASK = False # Unit tests using interfaces are not enclosed in a Flask route
interfaces = InterfaceHolder()
utils = TestUtils()
open(FileTests.TABLES_CLEARED_FILE,"w").write(str(False)) # Mark file tests as not having cleared tables
# Pass routeTests into other test sets to save time, they can also run without the arguments
# Create test suite
suite = unittest.TestSuite()
# Get lists of method names
loginMethods = LoginTests.__dict__.keys()
fileMethods = FileTests.__dict__.keys()
userMethods = UserTests.__dict__.keys()
#loginMethods = []
#fileMethods = [["test_file_submission"]]

# Set up sample users
UserTests.setupUserList()

for method in loginMethods:
    # If test method, add to suite
    if(method[0:4] == "test"):
        test =LoginTests(methodName=method)
        #test.addUtils(utils)
        #suite.addTest(test)

for method in fileMethods:
    # If test method, add to suite
    if(method[0:4] == "test"):
        test =FileTests(methodName=method,interfaces=interfaces)
        #test.addUtils(utils)
        #suite.addTest(test)

for method in userMethods:
    # If test method, add to suite
    if(method[0:4] == "test"):
        test =UserTests(methodName=method,interfaces=interfaces)
        test.addUtils(utils)
        suite.addTest(test)

print(str(suite.countTestCases()) + " tests in suite")

# Run tests and store results
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
