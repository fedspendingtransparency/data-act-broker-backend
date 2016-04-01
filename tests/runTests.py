import unittest
from loginTests import LoginTests
from fileTests import FileTests
from userTests import UserTests

# Create test suite
suite = unittest.TestSuite()
suite.addTests(unittest.makeSuite(LoginTests))
suite.addTests(unittest.makeSuite(FileTests))
suite.addTests(unittest.makeSuite(UserTests))

print("{} tests in suite".format(suite.countTestCases()))

# Run tests and store results
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
