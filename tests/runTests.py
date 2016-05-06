import unittest
from loginTests import LoginTests
from fileTests import FileTests
from userTests import UserTests
from jobTests import JobTests
from validatorTests import ValidatorTests
from fileTypeTests import FileTypeTests
import cProfile
import pstats

def runTests():
    PROFILE = False

    # Create test suite
    suite = unittest.TestSuite()

    suite.addTests(unittest.makeSuite(LoginTests))
    suite.addTests(unittest.makeSuite(FileTests))
    suite.addTests(unittest.makeSuite(UserTests))
    suite.addTests(unittest.makeSuite(ValidatorTests))
    suite.addTests(unittest.makeSuite(JobTests))
    suite.addTests(unittest.makeSuite(FileTypeTests))

    # to run a single test:
    #suite.addTest(JobTests('test_bad_values_job'))
    #suite.addTest(FileTypeTests('test_award_fin_mixed'))

    print("{} tests in suite".format(suite.countTestCases()))

    # Run tests and store results
    runner = unittest.TextTestRunner(verbosity=2)

    if PROFILE:
        cProfile.run("runner.run(suite)","stats")
        stats = pstats.Stats("stats")
        stats.sort_stats("tottime").print_stats(100)
    else:
        runner.run(suite)

if __name__ == '__main__':
    runTests()