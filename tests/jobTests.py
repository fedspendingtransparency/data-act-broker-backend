import unittest
import json
from sqlalchemy.exc import InvalidRequestError
from testUtils import TestUtils
from dataactcore.models.jobModels import Status, Type
from dataactcore.scripts.databaseSetup import runCommands
from dataactcore.scripts.setupValidationDB import setupValidationDB
from dataactcore.scripts.clearErrors import clearErrors
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder
from dataactvalidator.interfaces.stagingInterface import StagingInterface

class JobTests(unittest.TestCase):


    TABLE_POPULATED = False  # Gets set to true by the first test to populate the tables
    DROP_TABLES = False  # If true, staging tables are dropped after tests are run
    DROP_OLD_TABLES = False # If true, attempts to drop staging tables from previous runs
    INCLUDE_LONG_TESTS = False # If true, includes tests with over a million errors, can take about half an hour to run

    CREATE_VALIDATION_RULES = True # If true, replaces validation rules currently in validation database
    JOB_ID_FILE = "jobId.json"
    LAST_CLEARED_FILE = "lastClearedId"
    jobIdDict = {}
    passed = False # Gets set to True by each test that passes
    methodName = None # Used by each test to track which test is running
    jobTracker = InterfaceHolder.JOB_TRACKER
    errorInterface = InterfaceHolder.ERROR

    def __init__(self, methodName):
        """ Run scripts to clear the job tables and populate with a defined test set """
        super(JobTests, self).__init__(methodName=methodName)
        self.methodName = methodName

        if not self.TABLE_POPULATED:
            # Create staging database
            runCommands(StagingInterface.getCredDict(), [], "staging")
            self.stagingDb = InterfaceHolder.STAGING

            setupValidationDB()
            validationDB = InterfaceHolder.VALIDATION
            if(self.CREATE_VALIDATION_RULES):
                # Clear validation rules
                for fileType in ["award","award_financial","appropriations","procurement"]:
                    validationDB.removeRulesByFileType(fileType)
                    validationDB.removeColumnsByFileType(fileType)

            # Clear databases and run setup
            #clearJobs()
            clearErrors()

            # Define user
            user = 1

            # Get interface for job tracker
            self.jobTracker = InterfaceHolder.JOB_TRACKER

            # Create submissions and get IDs back
            submissionIDs = {}
            for i in range(1,17):
                submissionIDs[i] = TestUtils.insertSubmission(self.jobTracker)


            csvFiles = {"valid":{"filename":"testValid.csv","status":"ready","type":"csv_record_validation","submissionLocalId":1,"fileType":1},
                        "bad_upload":{"filename":"","status":"ready","type":"file_upload","submissionLocalId":2,"fileType":1},
                        "bad_prereq":{"filename":"","status":"ready","type":"csv_record_validation","submissionLocalId":2,"fileType":1},
                        "wrong_type":{"filename":"","status":"ready","type":"external_validation","submissionLocalId":4,"fileType":1},
                        "not_ready":{"filename":"","status":"finished","type":"csv_record_validation","submissionLocalId":5,"fileType":1},
                        "valid_upload":{"filename":"","status":"finished","type":"file_upload","submissionLocalId":6,"fileType":1},
                        "valid_prereq":{"filename":"testPrereq.csv","status":"ready","type":"csv_record_validation","submissionLocalId":6,"fileType":1},
                        "bad_values":{"filename":"testBadValues.csv","status":"ready","type":"csv_record_validation","submissionLocalId":8,"fileType":1},
                        "mixed":{"filename":"testMixed.csv","status":"ready","type":"csv_record_validation","submissionLocalId":9,"fileType":1},
                        "empty":{"filename":"testEmpty.csv","status":"ready","type":"csv_record_validation","submissionLocalId":10,"fileType":1},
                        "missing_header":{"filename":"testMissingHeader.csv","status":"ready","type":"csv_record_validation","submissionLocalId":11,"fileType":1},
                        "bad_header":{"filename":"testBadHeader.csv","status":"ready","type":"csv_record_validation","submissionLocalId":12,"fileType":2},
                        "many":{"filename":"testMany.csv","status":"ready","type":"csv_record_validation","submissionLocalId":11,"fileType":3},
                        "odd_characters":{"filename":"testOddCharacters.csv","status":"ready","type":"csv_record_validation","submissionLocalId":14,"fileType":2},
                        "many_bad":{"filename":"testManyBadValues.csv","status":"ready","type":"csv_record_validation","submissionLocalId":11,"fileType":4},
                        "rules":{"filename":"testRules.csv","status":"ready","type":"csv_record_validation","submissionLocalId":16,"fileType":1}}

            # Upload needed files to S3
            for key in csvFiles.keys():
                csvFiles[key]["s3Filename"] = TestUtils.uploadFile(csvFiles[key]["filename"],user)

            self.jobIdDict = {}

            sqlStatements = []
            for key in csvFiles.keys():
                # Create SQL statement and add to list
                statement = TestUtils.createJobStatement(str(Status.getStatus(csvFiles[key]["status"])), str(Type.getType(csvFiles[key]["type"])), str(submissionIDs[csvFiles[key]["submissionLocalId"]]), csvFiles[key]["s3Filename"], str(csvFiles[key]["fileType"]))
                jobId = self.jobTracker.runStatement(statement)
                try:
                    self.jobIdDict[key] = jobId.fetchone()[0]
                except InvalidRequestError:
                    # Problem getting result back, may happen for dependency statements
                    pass


            print(str(self.jobIdDict))
            # Last job number
            minJob = min(self.jobIdDict.values())
            lastJob = max(self.jobIdDict.values())

            # Save jobIdDict to file
            open(self.JOB_ID_FILE,"w").write(json.dumps(self.jobIdDict))

            # Create dependencies
            depStatements = ["INSERT INTO job_dependency (job_id, prerequisite_id) VALUES ("+str(self.jobIdDict["bad_prereq"])+", "+str(self.jobIdDict["bad_upload"])+")",
            "INSERT INTO job_dependency (job_id, prerequisite_id) VALUES ("+str(self.jobIdDict["valid_prereq"])+", "+str(self.jobIdDict["valid_upload"])+")"]

            for statement in depStatements:
                self.jobTracker.runStatement(statement)



            if(self.CREATE_VALIDATION_RULES):
                colIdDict = {}
                for fileId in range(1,5):
                    for columnId in range(1,6):
                        if(columnId < 3):
                            fieldType = 1
                        else:
                            fieldType = 4
                        columnName = "header_" + str(columnId)
                        statement = TestUtils.createColumnStatement(fileId, fieldType, columnName, "", (columnId != 3))
                        colId = validationDB.runStatement(statement)
                        colIdDict["header_"+str(columnId)+"_file_type_"+str(fileId)] = colId.fetchone()[0]

                ruleStatement = "INSERT INTO rule (file_column_id, rule_type_id, rule_text_1, description) VALUES ("+str(colIdDict["header_"+str(1)+"_file_type_"+str(3)])+", 5, 0, 'value 1 must be greater than zero'),("+str(colIdDict["header_"+str(1)+"_file_type_"+str(3)])+",3,13,'value 1 may not be 13'),("+str(colIdDict["header_"+str(5)+"_file_type_"+str(3)])+",1,'INT','value 5 must be an integer'),("+str(colIdDict["header_"+str(3)+"_file_type_"+str(3)])+",2,42,'value 3 must be equal to 42 if present'),("+str(colIdDict["header_"+str(1)+"_file_type_"+str(3)])+",4,100,'value 1 must be less than 100')"

                validationDB.runStatement(ruleStatement)

            if(self.DROP_OLD_TABLES):
                try:
                    firstJob = open(self.LAST_CLEARED_FILE,"r").read()
                except:
                    # If anything goes wrong, just clear from 0
                    firstJob = 0
                try:
                    if(int(firstJob) > minJob):
                        # This probably means sequence got reset and we started from 0, so clear all up to lastJob
                        firstJob = 0
                except:
                    # Could not cast as int
                    firstJob = 0


                print("Dropping staging tables from " + str(firstJob) + " to " + str(lastJob))
                # Remove existing tables from staging if they exist
                for jobId in range(int(firstJob)+1, lastJob+1):
                    self.stagingDb.dropTable("job"+str(jobId))

                open(self.LAST_CLEARED_FILE,"w").write(str(lastJob))
            JobTests.TABLE_POPULATED = True
        else:
            self.stagingDb = InterfaceHolder.STAGING
            # Read job ID dict from file
            self.jobIdDict = json.loads(open(self.JOB_ID_FILE,"r").read())

    def setUp(self):
        self.passed = False

    def test_valid_job(self):
        """ Test valid job """
        jobId = self.jobIdDict["valid"]
        self.passed = TestUtils.run_test(jobId,200,"finished",52,1,"complete",0,self)

    def test_rules(self):
        """ Test rules, should have one type failure and two value failures """
        jobId = self.jobIdDict["rules"]
        self.passed = TestUtils.run_test(jobId,200,"finished",350,1,"complete",5,self)

    def test_bad_values_job(self):
        # Test job with bad values
        jobId = self.jobIdDict["bad_values"]
        self.passed = TestUtils.run_test(jobId,200,"finished",5574,0,"complete",92,self)

    def test_many_bad_values_job(self):
        # Test job with many bad values
        if self.INCLUDE_LONG_TESTS:
            jobId = self.jobIdDict["many_bad"]
            self.passed = TestUtils.run_test(jobId,200,"finished",151665643,0,"complete",2302930,self)
        else:
            self.passed = True

    def test_mixed_job(self):
        """ Test mixed job """
        jobId = self.jobIdDict["mixed"]
        self.passed = TestUtils.run_test(jobId,200,"finished",99,3,"complete",1,self)

    def test_empty(self):
        """ Test empty file """
        jobId = self.jobIdDict["empty"]
        if TestUtils.USE_THREADS:
            status = 200
        else:
            status = 400
        self.passed = TestUtils.run_test(jobId,status,"invalid",False,False,"single_row_error",0,self)

        if not TestUtils.USE_THREADS:
            assert(self.response.json()["message"] == "CSV file must have a header")

    def test_missing_header(self):
        """ Test missing header in first row """
        jobId = self.jobIdDict["missing_header"]
        if TestUtils.USE_THREADS:
            status = 200
        else:
            status = 400
        self.passed = TestUtils.run_test(jobId,status,"invalid",False,False,"missing_header_error",0,self)

        if not TestUtils.USE_THREADS:
            assert(self.response.json()["message"] == "Header : header_5 is required")


    def test_bad_header(self):
        """ Test bad header value in first row """
        jobId = self.jobIdDict["bad_header"]
        if TestUtils.USE_THREADS:
            status = 200
        else:
            status = 400
        self.passed = TestUtils.run_test(jobId,status,"invalid",False,False,"bad_header_error",0,self)

        if not TestUtils.USE_THREADS:
            assert(self.response.json()["message"] == "Header : walrus not in CSV schema")

    def test_many_rows(self):
        """ Test many rows """
        if self.INCLUDE_LONG_TESTS:
            # Don't do this test when skipping long tests
            jobId = self.jobIdDict["many"]
            self.passed = TestUtils.run_test(jobId,200,"finished",52,22380,"complete",0,self)
        else:
            self.passed = True


    def test_odd_characters(self):
        """ Test potentially problematic characters """
        jobId = self.jobIdDict["odd_characters"]
        self.passed = TestUtils.run_test(jobId,200,"finished",165,5,"complete",2,self)

    def test_bad_id_job(self):
        """ Test job ID not found in job status table """
        jobId = -1
        self.passed = TestUtils.run_test(jobId,400,False,False,False,"job_error",0,self)

    def test_prereq_job(self):
        """ Test job with prerequisites finished """
        jobId = self.jobIdDict["valid_prereq"]
        self.passed = TestUtils.run_test(jobId,200,"finished",52,4,"complete",0,self)

    def test_bad_prereq_job(self):
        """ Test job with unfinished prerequisites """
        jobId = self.jobIdDict["bad_prereq"]
        self.passed = TestUtils.run_test(jobId,400,"ready",False,False,"job_error",0,self)

    def test_bad_type_job(self):
        """ Test job with wrong type """
        jobId = self.jobIdDict["wrong_type"]
        self.passed = TestUtils.run_test(jobId,400,"ready",False,False,"job_error",0,self)

    # TODO uncomment this unit test once jobs are labeled as ready
    # def test_finished_job(self):
        # """ Test job that is already finished """
        # jobId = self.jobIdDict["finished"]
        # self.run_test(jobId,400,"finished",False,False,"job_error",0)

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
