import unittest
from interfaces.stagingInterface import StagingInterface
from dataactcore.models.jobModels import Status, Type
from dataactcore.models import errorModels
import requests
import os
import inspect
import time
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from dataactcore.scripts.databaseSetup import runCommands
from scripts.setupValidationDB import setupValidationDB
from dataactcore.scripts.clearErrors import clearErrors
from interfaces.interfaceHolder import InterfaceHolder
from dataactcore.scripts.clearJobs import clearJobs
from sqlalchemy.exc import InvalidRequestError
import json

class JobTests(unittest.TestCase):

    BASE_URL = "http://127.0.0.1:5000"
    #BASE_URL = "http://52.90.92.100:5000"
    JSON_HEADER = {"Content-Type": "application/json"}
    TABLE_POPULATED = False  # Gets set to true by the first test to populate the tables
    DROP_TABLES = False  # If true, staging tables are dropped after tests are run
    USE_THREADS = False
    INCLUDE_LONG_TESTS = True
    UPLOAD_FILES = True
    JOB_ID_FILE = "jobId.json"
    LAST_CLEARED_FILE = "lastClearedId"
    jobIdDict = {}

    def __init__(self, methodName):
        """ Run scripts to clear the job tables and populate with a defined test set """
        super(JobTests, self).__init__(methodName=methodName)


        if not self.TABLE_POPULATED:


            # Create staging database
            runCommands(StagingInterface.getCredDict(), [], "staging")
            self.stagingDb = InterfaceHolder.STAGING

            # Clear databases and run setup
            clearJobs()
            clearErrors()
            setupValidationDB()

            # Define user
            user = 1
            # Upload needed files to S3

            s3FileNameValid = self.uploadFile("testValid.csv", user)
            s3FileNamePrereq = self.uploadFile("testPrereq.csv", user)
            s3FileNameBadValues = self.uploadFile("testBadValues.csv", user)
            s3FileNameMixed = self.uploadFile("testMixed.csv", user)
            s3FileNameEmpty = self.uploadFile("testEmpty.csv", user)
            s3FileNameMissingHeader = self.uploadFile("testMissingHeader.csv", user)
            s3FileNameBadHeader = self.uploadFile("testBadHeader.csv", user)
            s3FileNameMany = self.uploadFile("testMany.csv", user)
            s3FileNameOdd = self.uploadFile("testOddCharacters.csv", user)
            s3FileNameManyBad = self.uploadFile("testManyBadValues.csv", user)
            s3FileNameTestRules = self.uploadFile("testRules.csv", user)

            # Populate with a defined test set
            self.jobTracker = InterfaceHolder.JOB_TRACKER

            # Create submissions and get IDs back
            submissionIDs = {}
            for i in range(1,17):
                submissionIDs[i] = self.insertSubmission(self.jobTracker)

            # Create jobs
            sqlStatements = ["INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[1])+", '" + s3FileNameValid + "',1) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("file_upload")) + ","+str(submissionIDs[2])+",1) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[2])+",1) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("external_validation")) + ","+str(submissionIDs[4])+",1) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, file_type_id) VALUES (" + str(Status.getStatus("finished")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[5])+",1) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, file_type_id) VALUES (" + str(Status.getStatus("finished")) + "," + str(Type.getType("file_upload")) + ","+str(submissionIDs[6])+",1) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[6])+", '" + s3FileNamePrereq + "',1) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[8])+", '" + s3FileNameBadValues + "',1) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[9])+", '" + s3FileNameMixed + "',1) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[10])+", '" + s3FileNameEmpty + "',1) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[11])+", '" + s3FileNameMissingHeader + "',1) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[12])+", '" + s3FileNameBadHeader + "',2) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[11])+", '" + s3FileNameMany + "',3) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[14])+", '" + s3FileNameOdd + "',2) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[11])+", '" + s3FileNameManyBad + "',4) RETURNING job_id",
            "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[16])+", '" + s3FileNameTestRules + "',1) RETURNING job_id"
            ]

            self.jobIdDict = {}
            keyList = ["valid","bad_upload","bad_prereq","wrong_type","not_ready","valid_upload","valid_prereq","bad_values","mixed","empty","missing_header","bad_header","many","odd_characters","many_bad","rules"]
            index = 0

            for statement in sqlStatements:
                jobId = self.jobTracker.runStatement(statement)
                try:
                    self.jobIdDict[keyList[index]] = jobId.fetchone()[0]
                except InvalidRequestError:
                    # Problem getting result back, may happen for dependency statements
                    pass
                index += 1

            print(str(self.jobIdDict))
            # Last job number
            lastJob = max(self.jobIdDict.values())

            # Save jobIdDict to file
            open(self.JOB_ID_FILE,"w").write(json.dumps(self.jobIdDict))

            # Create dependencies
            depStatements = ["INSERT INTO job_dependency (job_id, prerequisite_id) VALUES ("+str(self.jobIdDict["bad_prereq"])+", "+str(self.jobIdDict["bad_upload"])+")",
            "INSERT INTO job_dependency (job_id, prerequisite_id) VALUES ("+str(self.jobIdDict["valid_prereq"])+", "+str(self.jobIdDict["valid_upload"])+")"]

            for statement in depStatements:
                self.jobTracker.runStatement(statement)

            validationDB = InterfaceHolder.VALIDATION

            validationDB.runStatement("DELETE FROM rule")
            validationDB.runStatement("DELETE FROM file_columns")
            fileColumnStatements = [[
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (1,1,'header 1','',True) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (1,1,'header 2','',True) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (1,4,'header 3','',False) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (1,4,'header 4','',True) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (1,4,'header 5','',True) RETURNING file_column_id"],[
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (2,1,'header 1','',True) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (2,1,'header 2','',True) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (2,4,'header 3','',False) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (2,4,'header 4','',True) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (2,4,'header 5','',True) RETURNING file_column_id"],[
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (3,1,'header 1','',True) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (3,1,'header 2','',True) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (3,4,'header 3','',False) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (3,4,'header 4','',True) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (3,4,'header 5','',True) RETURNING file_column_id"],[
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (4,1,'header 1','',True) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (4,1,'header 2','',True) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (4,4,'header 3','',False) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (4,4,'header 4','',True) RETURNING file_column_id",
                "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (4,4,'header 5','',True) RETURNING file_column_id"]]

            colIdDict = {}
            for fileType in range(0,4):
                for i in range(0,5):
                    colId = validationDB.runStatement(fileColumnStatements[fileType][i])
                    try:
                        colIdDict["header_"+str(i+1)+"_file_type_"+str(fileType+1)] = colId.fetchone()[0]
                    except InvalidRequestError as e:
                        # Could not get column ID
                        pass

            ruleStatement = "INSERT INTO rule (file_column_id, rule_type_id, rule_text_1, description) VALUES ("+str(colIdDict["header_"+str(1)+"_file_type_"+str(3)])+", 5, 0, 'value 1 must be greater than zero'),("+str(colIdDict["header_"+str(1)+"_file_type_"+str(3)])+",3,13,'value 1 may not be 13'),("+str(colIdDict["header_"+str(5)+"_file_type_"+str(3)])+",1,'INT','value 5 must be an integer'),("+str(colIdDict["header_"+str(3)+"_file_type_"+str(3)])+",2,42,'value 3 must be equal to 42 if present'),("+str(colIdDict["header_"+str(1)+"_file_type_"+str(3)])+",4,100,'value 1 must be less than 100')"

            validationDB.runStatement(ruleStatement)
            try:
                firstJob = open(self.LAST_CLEARED_FILE,"r").read()
            except:
                # If anything goes wrong, just clear from 0
                firstJob = 0

            if(firstJob > lastJob):
                # This probably means sequence got reset and we started from 0, so clear all up to lastJob
                firstJob = 0

            # Remove existing tables from staging if they exist
            for jobId in range(int(firstJob)+1, lastJob+1):
                self.stagingDb.dropTable("job"+str(jobId))

            open(self.LAST_CLEARED_FILE,"w").write(str(lastJob))
            JobTests.TABLE_POPULATED = True
        else:
            self.stagingDb = InterfaceHolder.STAGING
            # Read job ID dict from file
            self.jobIdDict = json.loads(open(self.JOB_ID_FILE,"r").read())

    @staticmethod
    def insertSubmission(jobTracker):
        """ Insert one submission into job tracker and get submission ID back """
        stmt = "INSERT INTO submission (datetime_utc) VALUES (0) RETURNING submission_id"
        response = jobTracker.runStatement(stmt)
        return response.fetchone()[0]

    @staticmethod
    def uploadFile(filename, user):
        """ Upload file to S3 and return S3 filename"""
        # Get bucket name
        bucketName = s3UrlHandler.getBucketNameFromConfig()

        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        fullPath = path + "/" + filename

        # Create file names for S3
        s3FileName = str(user) + "/" + filename

        if(JobTests.UPLOAD_FILES) :
            # Use boto to put files on S3
            s3conn = S3Connection()
            key = Key(s3conn.get_bucket(bucketName))
            key.key = s3FileName
            bytesWritten = key.set_contents_from_filename(fullPath)

            assert(bytesWritten > 0)
        return s3FileName

    def test_valid_job(self):
        """ Test valid job """
        jobId = self.jobIdDict["valid"]
        self.response = self.validateJob(jobId)

        assert(self.response.status_code == 200)
        self.waitOnJob(self.jobTracker, jobId, "finished")


        self.assertHeader(self.response)
        # Check that job is correctly marked as finished
        assert(self.jobTracker.getStatus(jobId) == Status.getStatus("finished"))
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == 37)

        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == True)
        assert(self.stagingDb.countRows(tableName) == 1)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("complete"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 0)

    def test_rules(self):
        """ Test rules, should have one type failure and two value failures """
        jobId = self.jobIdDict["rules"]
        self.response = self.validateJob(jobId)
        self.waitOnJob(self.jobTracker, jobId, "finished")
        assert(self.response.status_code == 200)
        self.assertHeader(self.response)
        # Check that job is correctly marked as finished
        assert(self.jobTracker.getStatus(jobId) == Status.getStatus("finished"))
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == 315)

        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == True)
        assert(self.stagingDb.countRows(tableName) == 1)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("complete"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 5)

    def test_bad_values_job(self):
        # Test job with bad values
        jobId = self.jobIdDict["bad_values"]
        self.response = self.validateJob(jobId)
        self.waitOnJob(self.jobTracker, jobId, "finished")
        assert(self.response.status_code == 200)
        self.assertHeader(self.response)
        # Check that job is correctly marked as finished
        assert(self.jobTracker.getStatus(jobId) == Status.getStatus("finished"))
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == 5413)
        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == True)
        assert(self.stagingDb.countRows(tableName) == 0)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("complete"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 92)

    def test_many_bad_values_job(self):
        # Test job with many bad values
        if not self.INCLUDE_LONG_TESTS:
            return
        jobId = self.jobIdDict["many_bad"]
        self.response = self.validateJob(jobId)
        self.waitOnJob(self.jobTracker, jobId, "finished")

        assert(self.response.status_code == 200)
        self.assertHeader(self.response)
        # Check that job is correctly marked as finished
        assert(self.jobTracker.getStatus(jobId) == Status.getStatus("finished"))
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == 133820283)
        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == True)
        assert(self.stagingDb.countRows(tableName) == 0)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("complete"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 2302930)

    def test_mixed_job(self):
        """ Test mixed job """
        jobId = self.jobIdDict["mixed"]
        self.response = self.validateJob(jobId)

        assert(self.response.status_code == 200)
        self.waitOnJob(self.jobTracker, jobId, "finished")

        self.assertHeader(self.response)
        # Check that job is correctly marked as finished
        assert(self.jobTracker.getStatus(jobId) == Status.getStatus("finished"))
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == 83)

        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == True)
        assert(self.stagingDb.countRows(tableName) == 3)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("complete"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 1)

    def test_empty(self):
        """ Test empty file """
        jobId = self.jobIdDict["empty"]
        self.response = self.validateJob(jobId)

        self.waitOnJob(self.jobTracker, jobId, "invalid")
        if JobTests.USE_THREADS:
            assert(self.response.status_code == 200)
        else:
            assert(self.response.status_code == 400)
        self.assertHeader(self.response)
        # Check that job is correctly marked as finished
        if not JobTests.USE_THREADS:
            assert(self.response.json()["message"] == "CSV file must have a header")
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == False)
        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == False)
        assert(self.stagingDb.countRows(tableName) == 0)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("single_row_error"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 0)

    def test_missing_header(self):
        """ Test missing header in first row """
        jobId = self.jobIdDict["missing_header"]
        self.response = self.validateJob(jobId)

        self.waitOnJob(self.jobTracker, jobId, "invalid")
        if JobTests.USE_THREADS:
            assert(self.response.status_code == 200)
        else:
            assert(self.response.status_code == 400)
        self.assertHeader(self.response)
        # Check that job is correctly marked as finished
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == False)

        if not JobTests.USE_THREADS:
            assert(self.response.json()["message"] == "Header : header 5 is required")
        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == False)
        assert(self.stagingDb.countRows(tableName) == 0)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("missing_header_error"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 0)

    def test_bad_header(self):
        """ Test bad header value in first row """
        jobId = self.jobIdDict["bad_header"]

        self.response = self.validateJob(jobId)
        if JobTests.USE_THREADS:
            assert(self.response.status_code == 200)
        else:
            assert(self.response.status_code == 400)
        self.waitOnJob(self.jobTracker, jobId, "invalid")
        self.assertHeader(self.response)
        # Check that job is correctly marked as finished
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == False)

        if not JobTests.USE_THREADS:
            assert(self.response.json()["message"] == "Header : walrus not in CSV schema")
        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == False)
        assert(self.stagingDb.countRows(tableName) == 0)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("bad_header_error"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 0)

    def test_many_rows(self):
        """ Test many rows """
        if not self.INCLUDE_LONG_TESTS:
            # Don't do this test when skipping long tests
            return
        jobId = self.jobIdDict["many"]
        self.response = self.validateJob(jobId)

        assert(self.response.status_code == 200)
        self.waitOnJob(self.jobTracker, jobId, "finished")
        self.assertHeader(self.response)
        # Check that job is correctly marked as finished
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == 37)

        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == True)
        assert(self.stagingDb.countRows(tableName) == 22380)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("complete"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 0)

    def test_odd_characters(self):
        """ Test potentially problematic characters """
        jobId = self.jobIdDict["odd_characters"]
        self.response = self.validateJob(jobId)
        self.waitOnJob(self.jobTracker, jobId, "finished")
        assert(self.response.status_code == 200)
        self.assertHeader(self.response)
        # Check that job is correctly marked as finished
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == 136)

        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == True)
        assert(self.stagingDb.countRows(tableName) == 5)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("complete"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 2)

    def test_bad_id_job(self):
        """ Test job ID not found in job status table """
        jobId = 2001
        self.response = self.validateJob(jobId)
        assert(self.response.status_code == 400)
        self.assertHeader(self.response)
        assert(self.response.json()["message"] == "Job ID not found in job_status table")
        assert(self.jobTracker.getReportPath(jobId) == False)

        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == False)
        assert(self.stagingDb.countRows(tableName) == 0)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("job_error"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 0)

    def test_prereq_job(self):
        """ Test job with prerequisites finished """
        jobId = self.jobIdDict["valid_prereq"]
        self.response = self.validateJob(jobId)
        self.waitOnJob(self.jobTracker, jobId, "finished")
        assert(self.response.status_code == 200)
        self.assertHeader(self.response)
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == 37)

        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == True)
        assert(self.stagingDb.countRows(tableName) == 4)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("complete"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 0)

    def test_bad_prereq_job(self):
        """ Test job with unfinished prerequisites """
        jobId = self.jobIdDict["bad_prereq"]
        self.response = self.validateJob(jobId)
        self.waitOnJob(self.jobTracker, jobId, "ready")
        assert(self.response.status_code == 400)
        self.assertHeader(self.response)
        assert(self.response.json()["message"] == "Prerequisites incomplete, job cannot be started")
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == False)

        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == False)
        assert(self.stagingDb.countRows(tableName) == 0)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("job_error"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 0)

    def test_bad_type_job(self):
        """ Test job with wrong type """
        jobId = self.jobIdDict["wrong_type"]
        self.response = self.validateJob(jobId)
        self.waitOnJob(self.jobTracker, jobId, "ready")
        assert(self.response.status_code == 400)
        self.assertHeader(self.response)
        assert(self.response.json()["message"] == "Wrong type of job for this service")
        assert(s3UrlHandler.getFileSize("errors/"+self.jobTracker.getReportPath(jobId)) == False)

        tableName = self.response.json()["table"]
        assert(self.stagingDb.tableExists(tableName) == False)
        assert(self.stagingDb.countRows(tableName) == 0)
        errorInterface = InterfaceHolder.ERROR
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("job_error"))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 0)

    # TODO uncomment this unit test once jobs are labeled as ready
    # def test_finished_job(self):
        # """ Test job that is already finished """
        # jobId = 5
        # self.response = self.validateJob(jobId)
        # assert(self.response.status_code == 400)
        # self.assertHeader(self.response)
        # assert(self.response.json()["message"] == "Job is not ready")
        # assert(s3UrlHandler.getFileSize(self.jobTracker.getReportPath(jobId)) == False)

        # tableName = self.response.json()["table"]
        # assert(self.stagingDb.tableExists(tableName) == False)
        # assert(self.stagingDb.countRows(tableName) == 0)
        # self.dropTables(tableName)
        # errorInterface = InterfaceHolder.ERROR
        # assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus("job_error"))
        # assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == 0)

    @staticmethod
    def assertHeader(response):
        """ Assert that content type header exists and is json """
        assert("Content-Type" in response.headers)
        assert(response.headers["Content-Type"] == "application/json")

    @staticmethod
    def waitOnJob(jobTracker, jobId, status):
        currentID = Status.getStatus("running")
        targetStatus = Status.getStatus(status)
        if JobTests.USE_THREADS:
            while jobTracker.getStatus(jobId) == currentID:
                time.sleep(1)
            assert(targetStatus == jobTracker.getStatus(jobId))
        else:
            assert(targetStatus == jobTracker.getStatus(jobId))
            return

    @staticmethod
    def validateJob(jobId):
        """ Send request to validate specified job """
        if JobTests.USE_THREADS:
            url = "/validate_threaded/"
        else:
            url = "/validate/"

        return requests.request(method="POST", url=JobTests.BASE_URL + url, data=JobTests.jobJson(jobId), headers=JobTests.JSON_HEADER)

    def setUp(self):
        self.jobTracker = InterfaceHolder.JOB_TRACKER
        self.errorInterface = InterfaceHolder.ERROR

    def tearDown(self):
        try:
            self.dropTables(self.response.json()["table"])
        except AttributeError:
            # Table not specified, generally this means the job didn't run
            pass

    def dropTables(self, table):
        if self.DROP_TABLES:
            stagingDb = InterfaceHolder.STAGING
            stagingDb.dropTable(table)
            return True
        else:
            return False

    @staticmethod
    def jobJson(jobId):
        """ Create JSON to hold jobId """
        return '{"job_id":'+str(jobId)+'}'
