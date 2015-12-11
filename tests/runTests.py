import unittest, inspect
from jobTests import JobTests
# Create test suite
suite = unittest.TestSuite()
# Get lists of method names
jobMethods = inspect.getmembers(JobTests, predicate=inspect.ismethod)
for method in jobMethods:
    # If test method, add to suite
    if(method[0][0:4] == "test"):
        test =JobTests(methodName=method[0])
        suite.addTest(test)

print(str(suite.countTestCases()) + " tests in suite")

# Run tests and store results
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)