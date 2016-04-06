import unittest
from loginTests import LoginTests
from fileTests import FileTests
from userTests import UserTests
import cProfile
import pstats

PROFILE = False

# Create test suite
suite = unittest.TestSuite()

suite.addTests(unittest.makeSuite(LoginTests))
suite.addTests(unittest.makeSuite(FileTests))
suite.addTests(unittest.makeSuite(UserTests))
# to run a single test:
#suite.addTest(FileTests('test_check_status'))

print("{} tests in suite".format(suite.countTestCases()))

# Run tests and store results
runner = unittest.TextTestRunner(verbosity=2)

if PROFILE:
    cProfile.run("runner.run(suite)","stats")
    stats = pstats.Stats("stats")
    stats.sort_stats("tottime").print_stats(100)
else:
    runner.run(suite)
