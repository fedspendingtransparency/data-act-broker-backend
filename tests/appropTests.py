import unittest
import json
from sqlalchemy.exc import InvalidRequestError
from dataactcore.models.jobModels import Status, Type
from dataactcore.scripts.databaseSetup import runCommands
from dataactvalidator.interfaces.stagingInterface import StagingInterface
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder
from testUtils import TestUtils

class AppropTests(unittest.TestCase):

    TABLE_POPULATED = False
    JOB_ID_FILE = "appropJobIds.json"
    jobIdDict = {}

    def __init__(self, methodName):
        """ Run scripts to clear the job tables and populate with a defined test set """
        super(AppropTests, self).__init__(methodName=methodName)
        self.methodName = methodName

        self.jobTracker = InterfaceHolder.JOB_TRACKER

        if not self.TABLE_POPULATED:
            # Last job number
            lastJob = 100

            # Create staging database
            runCommands(StagingInterface.getCredDict(), [], "staging")
            self.stagingDb = InterfaceHolder.STAGING

            # Define user
            user = 1
            # Upload needed files to S3

            s3FileNameValid = TestUtils.uploadFile("appropValid.csv", user)
            s3FileNameMixed = TestUtils.uploadFile("appropMixed.csv", user)
            s3FileNameTas = TestUtils.uploadFile("tasMixed.csv", user)

            # Create submissions and get IDs back
            submissionIDs = {}
            for i in range(1,4):
                submissionIDs[i] = TestUtils.insertSubmission(self.jobTracker)

            # Create jobs
            sqlStatements = ["INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[1])+", '" + s3FileNameValid + "',3) RETURNING job_id",
                             "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[2])+", '" + s3FileNameMixed + "',3) RETURNING job_id",
                             "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[3])+", '" + s3FileNameTas + "',3) RETURNING job_id"]

            self.jobIdDict = {}
            keyList = ["valid","mixed","tas"]
            index = 0
            for statement in sqlStatements:
                jobId = self.jobTracker.runStatement(statement)
                try:
                    self.jobIdDict[keyList[index]] = jobId.fetchone()[0]
                except InvalidRequestError:
                    # Problem getting result back, may happen for dependency statements
                    pass
                index += 1

            # Save jobIdDict to file
            print(self.jobIdDict)
            open(self.JOB_ID_FILE,"w").write(json.dumps(self.jobIdDict))

            # Load fields and rules
            import dataactvalidator.scripts.loadApprop

            # Remove existing tables from staging if they exist
            for jobId in self.jobIdDict.values():
                self.stagingDb.dropTable("job"+str(jobId))

            AppropTests.TABLE_POPULATED = True
        else:
            self.stagingDb = InterfaceHolder.STAGING
            # Read job ID dict from file
            self.jobIdDict = json.loads(open(self.JOB_ID_FILE,"r").read())

    def test_approp_valid(self):
        """ Test valid job """
        jobId = self.jobIdDict["valid"]
        self.passed = TestUtils.run_test(jobId,200,"finished",52,20,"complete",0,self)

    def test_approp_mixed(self):
        """ Test mixed job with 5 rows failing """
        jobId = self.jobIdDict["mixed"]
        self.passed = TestUtils.run_test(jobId,200,"finished",5606,15,"complete",47,self)

    def test_tas_mixed(self):
        """ Test TAS validation """
        jobId = self.jobIdDict["tas"]
        self.passed = TestUtils.run_test(jobId,200,"finished",1597,2,"complete",5,self)

    def setUp(self):
        self.passed = False

    def tearDown(self):
        if not self.passed:
            print("Test failed: " + self.methodName)
            # Runs only for tests that fail
            print(self.response.status_code)
            print(self.response.json()["errorType"])
            print(self.response.json()["message"])
            print(self.response.json()["trace"])
            print(self.response.json()["wrappedType"])
            print(self.response.json()["wrappedMessage"])