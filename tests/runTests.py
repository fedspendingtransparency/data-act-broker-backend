import unittest
from jobTests import JobTests
from validatorTests import ValidatorTests
from fileTypeTests import FileTypeTests

def runTests():
    """Create and run validation test suite."""
    suite = unittest.TestSuite()
    suite.addTests(unittest.makeSuite(ValidatorTests))
    suite.addTests(unittest.makeSuite(JobTests))
    suite.addTests(unittest.makeSuite(FileTypeTests))
    # to run a single test:
    #suite.addTest(FileTypeTests('test_award_fin_mixed'))

    print("{} tests in suite".format(suite.countTestCases()))
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

if __name__ == '__main__':
    runTests()
