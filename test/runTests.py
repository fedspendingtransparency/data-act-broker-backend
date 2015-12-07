from test.testUtils import TestUtils
from test.loginTests import LoginTests
from test.fileTests import FileTests
import unittest
import sys
import inspect

utils = TestUtils()
# Pass routeTests into other test sets to save time, they can also run without the arguments
# Create test suite
suite = unittest.TestSuite()
# Get lists of method names
loginMethods = inspect.getmembers(LoginTests, predicate=inspect.ismethod)
fileMethods = inspect.getmembers(FileTests, predicate=inspect.ismethod)
for method in loginMethods:
    # If test method, add to suite
    if(method[0][0:4] == "test"):
        test =LoginTests(methodName=method[0])
        test.addUtils(utils)
        suite.addTest(test)

for method in fileMethods:
    # If test method, add to suite
    if(method[0][0:4] == "test"):
        test =FileTests(methodName=method[0])
        test.addUtils(utils)
        suite.addTest(test)

print(str(suite.countTestCases()) + " tests in suite")

# Run tests and store results
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
#print(unittest.TextTestResult.testsRun)
#print(str(unittest.TextTestResult.errors))
#result = unittest.TestResult()
#suite.run(result)

#unittest.defaultTestLoader.loadTestsFromTestCase(loginTests)
#unittest.defaultTestLoader.loadTestsFromTestCase(fileTests)
#unittest.main()