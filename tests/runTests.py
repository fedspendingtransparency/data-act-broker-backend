import unittest, inspect
from jobTests import JobTests
from validatorTests import ValidatorTests
from interfaces.stagingInterface import StagingInterface

# Create test suite
suite = unittest.TestSuite()
# Get lists of method names

validatorMethods = inspect.getmembers(ValidatorTests, predicate=inspect.ismethod)
jobMethods = inspect.getmembers(JobTests, predicate=inspect.ismethod)

for method in validatorMethods:
    # If test method, add to suite
    if(method[0][0:4] == "test"):
        test =ValidatorTests(methodName=method[0])
        suite.addTest(test)

for method in jobMethods:
    # If test method, add to suite
    if(method[0][0:4] == "test"):
        test =JobTests(methodName=method[0])
        suite.addTest(test)


print(str(suite.countTestCases()) + " tests in suite")

# Run tests and store results
runner = unittest.TextTestRunner(verbosity=2)
try:
    print("Running tests")
    runner.run(suite)
finally:
    # Drop staging tables
    print("Dropping tables")
    stagingDb = StagingInterface()
    tables = stagingDb.getTables()
    for table in tables:
        print("Dropping table "+table)
        stagingDb.runStatement("DROP TABLE " + table)