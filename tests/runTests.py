import unittest
from loginTests import LoginTests
from fileTests import FileTests
from userTests import UserTests

# Create test suite
suite = unittest.TestSuite()
# Get lists of method names
loginMethods = LoginTests.__dict__.keys()
fileMethods = FileTests.__dict__.keys()
userMethods = UserTests.__dict__.keys()

for method in loginMethods:
    # If test method, add to suite
    if(method[0:4] == "test"):
        test =LoginTests(methodName=method)
        suite.addTest(test)

for method in fileMethods:
    # If test method, add to suite
    if(method[0:4] == "test"):
        test =FileTests(methodName=method)
        suite.addTest(test)

for method in userMethods:
    # If test method, add to suite
    if(method[0:4] == "test"):
        test =UserTests(methodName=method)
        suite.addTest(test)

print(str(suite.countTestCases()) + " tests in suite")

# Run tests and store results
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
