import unittest
from interfaces.interfaceHolder import InterfaceHolder

class StagingTests(unittest.TestCase):
    BASE_URL = "http://127.0.0.1:5000"
    JSON_HEADER = {"Content-Type": "application/json"}

    # May need some setup code to populate validation database, ideally this would live in ValidationTests

    def setup(self):
        self.stagingDB = InterfaceHolder.STAGING

    def test_write_job(self):
        jobId = 1
        tableName = self.stagingDB.createTable("Award",jobId)
        assert(tableName != False)
        data = ["?","?"]
        assert(self.stagingDB.writeData(tableName, data) == True)
