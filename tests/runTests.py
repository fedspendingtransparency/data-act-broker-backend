import unittest
from loginTests import LoginTests
from fileTests import FileTests
from userTests import UserTests
from jobTests import JobTests
from validatorTests import ValidatorTests
from fileTypeTests import FileTypeTests
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
    #suite.addTest(FileTests('test_bad_quarter_or_month'))
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
