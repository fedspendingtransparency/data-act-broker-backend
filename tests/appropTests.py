import unittest
from interfaces.stagingInterface import StagingInterface
from dataactcore.models.jobModels import Status, Type
from dataactcore.models import errorModels
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.scripts.databaseSetup import runCommands
from interfaces.interfaceHolder import InterfaceHolder
from sqlalchemy.exc import InvalidRequestError
import json
from tests.jobTests import JobTests
from filestreaming.schemaLoader import SchemaLoader

class AppropTests(unittest.TestCase):

    TABLE_POPULATED = False
    JOB_ID_FILE = "appropJobIds.json"
    jobIdDict = {}

    def __init__(self, methodName):
        """ Run scripts to clear the job tables and populate with a defined test set """
        super(AppropTests, self).__init__(methodName=methodName)
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

            s3FileNameValid = JobTests.uploadFile("appropValid.csv", user)
            s3FileNameMixed = JobTests.uploadFile("appropMixed.csv", user)

            self.jobTracker = InterfaceHolder.JOB_TRACKER

            # Create submissions and get IDs back
            submissionIDs = {}
            for i in range(1,3):
                submissionIDs[i] = JobTests.insertSubmission(self.jobTracker)

            # Create jobs
            sqlStatements = ["INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[1])+", '" + s3FileNameValid + "',3) RETURNING job_id",
                             "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[2])+", '" + s3FileNameMixed + "',3) RETURNING job_id"]

            self.jobIdDict = {}
            keyList = ["valid","mixed"]
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

            SchemaLoader.loadFields("appropriations","appropriationsFields.csv")

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
        self.response = JobTests.validateJob(jobId)
        if(self.response.status_code != 200):
            print(self.response.status_code)
            print(self.response.json()["errorType"])
            print(self.response.json()["message"])
            print(self.response.json()["trace"])
            print(self.response.json()["wrappedType"])
            print(self.response.json()["wrappedMessage"])
        assert(self.response.status_code == 200)

        JobTests.waitOnJob(self.jobTracker, jobId, "finished")

        JobTests.assertHeader(self.response)
        # Check that job is correctly marked as finished
        assert(self.jobTracker.getStatus(jobId) == Status.getStatus("finished"))
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == 37)

        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == True)
        assert(self.stagingDb.countRows(tableName) == 20)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("complete"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 0)

    def test_approp_mixed(self):
        """ Test mixed job with 3 rows failing """
        jobId = self.jobIdDict["mixed"]
        self.response = JobTests.validateJob(jobId)
        if(self.response.status_code != 200):
            print(self.response.status_code)
            print(self.response.json()["errorType"])
            print(self.response.json()["message"])
            print(self.response.json()["trace"])
            print(self.response.json()["wrappedType"])
            print(self.response.json()["wrappedMessage"])
        assert(self.response.status_code == 200)

        JobTests.waitOnJob(self.jobTracker, jobId, "finished")

        JobTests.assertHeader(self.response)
        # Check that job is correctly marked as finished
        assert(self.jobTracker.getStatus(jobId) == Status.getStatus("finished"))
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == 2617)

        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == True)
        assert(self.stagingDb.countRows(tableName) == 15)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("complete"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 31)