import unittest
from loginTests import LoginTests
from fileTests import FileTests
from userTests import UserTests

# Create test suite
suite = unittest.TestSuite()

suite.addTests(unittest.makeSuite(LoginTests))


print("{} tests in suite".format(suite.countTestCases()))

# Run tests and store results
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

fileSuite = unittest.TestSuite()
fileSuite.addTests(unittest.makeSuite(FileTests))
runner.run(fileSuite)

userSuite = unittest.TestSuite()
userSuite.addTests(unittest.makeSuite(UserTests))
runner.run(userSuite)