import unittest
import json
from sqlalchemy.exc import InvalidRequestError
from testUtils import TestUtils
from dataactcore.models.jobModels import Status, JobDependency
from dataactcore.scripts.databaseSetup import runCommands
from dataactcore.scripts.clearErrors import clearErrors
from dataactvalidator.interfaces.validatorStagingInterface import ValidatorStagingInterface
from dataactvalidator.scripts.setupValidationDB import setupValidationDB
from dataactvalidator.scripts.setupStagingDB import setupStaging
from dataactvalidator.models.validationModels import Rule

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

    def __init__(self, methodName,interfaces):
        """ Run scripts to clear the job tables and populate with a defined test set """
        super(JobTests, self).__init__(methodName=methodName)
        self.interfaces = interfaces
        self.methodName = methodName
        self.jobTracker = interfaces.jobDb
        self.errorInterface = interfaces.errorDb

        if not self.TABLE_POPULATED:
            # Create staging database
            setupStaging()
            self.stagingDb = interfaces.stagingDb

            setupValidationDB()
            validationDB = interfaces.validationDb
            if(self.CREATE_VALIDATION_RULES):
                # Clear validation rules
                for fileType in ["award","award_financial","appropriations","program_activity"]:
                    validationDB.removeRulesByFileType(fileType)
                    validationDB.removeColumnsByFileType(fileType)

            # Clear databases and run setup
            #clearJobs()
            clearErrors()

            # Define user
            user = 1

            # Get interface for job tracker
            self.jobTracker = interfaces.jobDb

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
                        "rules":{"filename":"testRules.csv","status":"ready","type":"csv_record_validation","submissionLocalId":16,"fileType":3}}

            # Upload needed files to S3
            print("Uploading files")
            for key in csvFiles.keys():
                csvFiles[key]["s3Filename"] = TestUtils.uploadFile(csvFiles[key]["filename"],user)

            self.jobIdDict = {}
            self.subIdDict = {}

            sqlStatements = []
            print("Inserting jobs")
            for key in csvFiles.keys():
                # Create SQL statement and add to list
                job = TestUtils.addJob(str(self.jobTracker.getStatusId(csvFiles[key]["status"])), str(self.jobTracker.getTypeId(csvFiles[key]["type"])), str(submissionIDs[csvFiles[key]["submissionLocalId"]]), csvFiles[key]["s3Filename"], str(csvFiles[key]["fileType"]),self.jobTracker.session)
                if(job.job_id == None):
                    # Failed to commit job correctly
                    raise Exception("".join(["Job for ", str(key), " did not get an id back"]))
                self.jobIdDict[key] = job.job_id
                self.subIdDict[key] = self.jobTracker.getSubmissionId(job.job_id)

            print(str(self.subIdDict))
            # Last job number
            minJob = min(self.jobIdDict.values())
            lastJob = max(self.jobIdDict.values())

            # Save jobIdDict to file
            open(self.JOB_ID_FILE,"w").write(json.dumps(self.jobIdDict))

            # Create dependencies
            dependencies = [JobDependency(job_id = str(self.jobIdDict["bad_prereq"]), prerequisite_id = str(self.jobIdDict["bad_upload"])),
                            JobDependency(job_id = str(self.jobIdDict["valid_prereq"]), prerequisite_id = str(self.jobIdDict["valid_upload"]))
                            ]

            for dependency in dependencies:
                self.jobTracker.session.add(dependency)
            self.jobTracker.session.commit()


            if(self.CREATE_VALIDATION_RULES):
                colIdDict = {}
                for fileId in range(1,5):
                    for columnId in range(1,6):
                        if(columnId < 3):
                            fieldType = 1
                        else:
                            fieldType = 4
                        columnName = "header_" + str(columnId)
                        column = TestUtils.addFileColumn(fileId, fieldType, columnName, "", (columnId != 3), self.interfaces.validationDb.session)
                        if(column.file_column_id is None):
                            raise Exception("File column did not get an ID back")
                        colIdDict["header_"+str(columnId)+"_file_type_"+str(fileId)] = column.file_column_id

                rules = [Rule(file_column_id = str(colIdDict["".join(["header_",str(1),"_file_type_",str(3)])]),rule_type_id = 5, rule_text_1 = 0, description =  'value 1 must be greater than zero'),
                         Rule(file_column_id = str(colIdDict["".join(["header_",str(1),"_file_type_",str(3)])]),rule_type_id = 3, rule_text_1 = 13, description =  'value 1 may not be 13'),
                         Rule(file_column_id = str(colIdDict["".join(["header_",str(5),"_file_type_",str(3)])]),rule_type_id = 1, rule_text_1 = "INT", description =  'value 5 must be an integer'),
                         Rule(file_column_id = str(colIdDict["".join(["header_",str(3),"_file_type_",str(3)])]),rule_type_id = 2, rule_text_1 = 42, description =  'value 3 must be equal to 42 if present'),
                         Rule(file_column_id = str(colIdDict["".join(["header_",str(1),"_file_type_",str(3)])]),rule_type_id = 4, rule_text_1 = 100, description =  'value 1 must be less than 100')
                         ]

                for rule in rules:
                    validationDB.session.add(rule)
                validationDB.session.commit()

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
                    try:
                        print("Dropping table " + str(jobId))
                        self.stagingDb.dropTable("job"+str(jobId))
                    except Exception as e:
                        # Could not drop table
                        print(str(e))
                        # Close and replace session
                        self.stagingDb.session.close()
                        self.stagingDb.session = self.stagingDb.Session()

                open(self.LAST_CLEARED_FILE,"w").write(str(lastJob))
            JobTests.TABLE_POPULATED = True
        else:
            self.stagingDb = interfaces.stagingDb
            # Read job ID dict from file
            self.jobIdDict = json.loads(open(self.JOB_ID_FILE,"r").read())

    def setUp(self):
        self.passed = False

    def test_valid_job(self):
        """ Test valid job """
        jobId = self.jobIdDict["valid"]
        self.passed = TestUtils.run_test(jobId,200,"finished",52,1,"complete",0,self)

    def test_rules(self):
        """ Test rules, should have one type failure and four value failures """
        jobId = self.jobIdDict["rules"]
        self.passed = TestUtils.run_test(jobId,200,"finished",350,1,"complete",5,self)

    def test_bad_values_job(self):
        # Test job with bad values
        jobId = self.jobIdDict["bad_values"]
        self.passed = TestUtils.run_test(jobId,200,"finished",5474,0,"complete",90,self)

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
        """ Test bad header value in first row, should now just be ignored """
        jobId = self.jobIdDict["bad_header"]
        status = 200
        self.passed = TestUtils.run_test(jobId,status,"finished",52,1,"complete",0,self)

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
        self.passed = TestUtils.run_test(jobId,200,"finished",99,6,"complete",1,self)

    def test_bad_id_job(self):
        """ Test job ID not found in job status table """
        jobId = -1
        self.passed = TestUtils.run_test(jobId,400,False,False,False,False,0,self)

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
            stagingDb = self.interfaces.stagingDb
            try:
                stagingDb.dropTable(table)
                return True
            except Exception as e:
                # Could not drop table
                print(str(e))
                # Close and replace session
                stagingDb.session.close()
                stagingDb.session = stagingDb.Session()
                return False
        else:
            return False
