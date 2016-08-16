import unittest
from tests.integration.loginTests import LoginTests
from tests.integration.fileTests import FileTests
from tests.integration.userTests import UserTests
from tests.integration.jobTests import JobTests
from tests.integration.validatorTests import ValidatorTests
from tests.integration.fileTypeTests import FileTypeTests
import cProfile
import pstats
import xmlrunner
import sys, getopt

def runTests(argv=''):
    PROFILE = False
    XMLresults = False

    # Get command line arg to determine output
    try:
        opts, args = getopt.getopt(argv,"o:")
    except getopt.GetoptError:
        XMLresults = False
    for opt, arg in opts:
      if opt == '-o' and arg == "XML":
        XMLresults = True
      else: 
        XMLresults = False

    # Create test suite
    suite = unittest.TestSuite()

    suite.addTests(unittest.makeSuite(LoginTests))
    suite.addTests(unittest.makeSuite(FileTests))
    suite.addTests(unittest.makeSuite(UserTests))
    suite.addTests(unittest.makeSuite(ValidatorTests))
    suite.addTests(unittest.makeSuite(JobTests))
    suite.addTests(unittest.makeSuite(FileTypeTests))

    # to run a single test:
    #suite.addTest(FileTests('test_check_status'))
    #suite.addTest(FileTypeTests('test_approp_mixed'))

    print("{} tests in suite".format(suite.countTestCases()))

    # Run tests and store results
    if XMLresults:
        runner = xmlrunner.XMLTestRunner(output='test-reports')
    else:
        runner = unittest.TextTestRunner(verbosity=2)

    if PROFILE:
        # Creating globals to be accessible to cProfile
        global profileRunner
        global profileSuite
        profileRunner = runner
        profileSuite = suite
        cProfile.run("profileRunner.run(profileSuite)","stats")
        stats = pstats.Stats("stats")
        stats.sort_stats("tottime").print_stats(100)
    else: 
        runner.run(suite)

if __name__ == '__main__':
    runTests(sys.argv[1:])
