import unittest, inspect
from jobTests import JobTests
from validatorTests import ValidatorTests
from appropTests import AppropTests
import cProfile
import pstats

def runTests():
    runMany = False # True to run the test suite multiple times

    # Create test suite
    suite = unittest.TestSuite()

    # Get lists of method names

    validatorMethods = inspect.getmembers(ValidatorTests, predicate=inspect.ismethod)
    jobMethods = inspect.getmembers(JobTests, predicate=inspect.ismethod)

    #validatorMethods = []
    #jobMethods = [] #[["test_valid_job"]]

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



    #print(str(suite.countTestCases()) + " tests in suite")

    # Run tests and store results
    runner = unittest.TextTestRunner(verbosity=2)
    if(runMany):
        for i in range(0,100):
            result = runner.run(suite)
            if(len(result.errors) > 0 or len(result.failures) > 0):
                raise Exception("Test Failed")
    else:
        result = runner.run(suite)

    appropSuite = unittest.TestSuite()
    appropMethods = inspect.getmembers(AppropTests, predicate=inspect.ismethod)
    #appropMethods = [["test_tas_mixed"]]

    for method in appropMethods:
        # If test method, add to suite
        if(method[0][0:4] == "test"):
            test =AppropTests(methodName=method[0])
            appropSuite.addTest(test)

    appropResult = runner.run(appropSuite)


if __name__ == '__main__':
    #runTests()
    cProfile.run("runTests()","stats")
    stats = pstats.Stats("stats")
    stats.sort_stats("cumulative").print_stats(100)