import unittest
import json
from sqlalchemy.exc import InvalidRequestError
from dataactcore.models.jobModels import Status, Type
from dataactcore.models.validationModels import TASLookup
from dataactcore.scripts.databaseSetup import runCommands
from dataactvalidator.interfaces.validatorStagingInterface import ValidatorStagingInterface
from dataactvalidator.filestreaming.schemaLoader import SchemaLoader
from dataactvalidator.scripts.tasSetup import loadTAS
from testUtils import TestUtils


class FileTypeTests(unittest.TestCase):

    TABLE_POPULATED = False
    FORCE_TAS_LOAD = False # If true, forces TAS to reload even when table is populated
    JOB_ID_FILE = "appropJobIds.json"
    jobIdDict = {}

    def __init__(self, methodName,interfaces):
        """ Run scripts to clear the job tables and populate with a defined test set """
        super(FileTypeTests, self).__init__(methodName=methodName)
        self.methodName = methodName

        self.jobTracker = interfaces.jobDb
        self.interfaces = interfaces
        if not self.TABLE_POPULATED:
            print("Defining jobs")
            # Last job number
            lastJob = 100

            # Create staging database
            runCommands(ValidatorStagingInterface.getCredDict(), [], "staging")
            self.stagingDb = interfaces.stagingDb

            # Define user
            user = 1
            # Upload needed files to S3

            s3FileNameValid = TestUtils.uploadFile("appropValid.csv", user)
            s3FileNameMixed = TestUtils.uploadFile("appropMixed.csv", user)
            s3FileNameTas = TestUtils.uploadFile("tasMixed.csv", user)
            s3FileNameProgramValid = TestUtils.uploadFile("programActivityValid.csv", user)
            s3FileNameProgramMixed = TestUtils.uploadFile("programActivityMixed.csv", user)
            s3FileNameAwardFinValid = TestUtils.uploadFile("awardFinancialValid.csv", user)
            s3FileNameAwardFinMixed = TestUtils.uploadFile("awardFinancialMixed.csv", user)
            s3FileNameAwardValid = TestUtils.uploadFile("awardValid.csv", user)
            s3FileNameAwardMixed = TestUtils.uploadFile("awardMixed.csv", user)

            # Create submissions and get IDs back
            submissionIDs = {}
            for i in range(1,10):
                submissionIDs[i] = TestUtils.insertSubmission(self.jobTracker)

            # Create jobs
            sqlStatements = ["INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[1])+", '" + s3FileNameValid + "',3) RETURNING job_id",
                             "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[2])+", '" + s3FileNameMixed + "',3) RETURNING job_id",
                             "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[3])+", '" + s3FileNameTas + "',3) RETURNING job_id",
                             "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[4])+", '" + s3FileNameProgramValid + "',4) RETURNING job_id",
                             "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[5])+", '" + s3FileNameProgramMixed + "',4) RETURNING job_id",
                             "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[6])+", '" + s3FileNameAwardFinValid + "',2) RETURNING job_id",
                             "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[7])+", '" + s3FileNameAwardFinMixed + "',2) RETURNING job_id",
                             "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[8])+", '" + s3FileNameAwardValid + "',1) RETURNING job_id",
                             "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ","+str(submissionIDs[9])+", '" + s3FileNameAwardMixed + "',1) RETURNING job_id"]

            self.jobIdDict = {}
            keyList = ["valid","mixed","tas","programValid","programMixed","awardFinValid","awardFinMixed","awardValid","awardMixed"]
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
            print("Loading definitions")
            self.load_definitions(self.interfaces)

            # Remove existing tables from staging if they exist
            for jobId in self.jobIdDict.values():
                try:
                    self.stagingDb.dropTable("job"+str(jobId))
                except Exception as e:
                    # Failed to drop table
                    print(str(e))
                    # Close and replace session
                    self.stagingDb.session.close()
                    self.stagingDb.session = self.stagingDb.Session()

            FileTypeTests.TABLE_POPULATED = True
        else:
            self.stagingDb = self.interfaces.stagingDb
            # Read job ID dict from file
            self.jobIdDict = json.loads(open(self.JOB_ID_FILE,"r").read())

    @staticmethod
    def load_definitions(interfaces):
        SchemaLoader.loadFields("appropriations","appropriationsFields.csv")
        SchemaLoader.loadRules("appropriations","appropriationsRules.csv")
        SchemaLoader.loadFields("procurement","programActivityFields.csv")
        SchemaLoader.loadFields("award_financial","awardFinancialFields.csv")
        SchemaLoader.loadFields("award","awardFields.csv")
        if(interfaces.validationDb.session.query(TASLookup).count() == 0 or FileTypeTests.FORCE_TAS_LOAD):
            # TAS table is empty, load it
            print("Loading TAS")
            loadTAS("all_tas_betc.csv")

    def test_approp_valid(self):
        """ Test valid job """
        jobId = self.jobIdDict["valid"]
        self.passed = TestUtils.run_test(jobId,200,"finished",52,20,"complete",0,self)

    def test_approp_mixed(self):
        """ Test mixed job with some rows failing """
        jobId = self.jobIdDict["mixed"]
        self.passed = TestUtils.run_test(jobId,200,"finished",5606,15,"complete",47,self)

    def test_tas_mixed(self):
        """ Test TAS validation """
        jobId = self.jobIdDict["tas"]
        self.passed = TestUtils.run_test(jobId,200,"finished",1597,2,"complete",5,self)

    def test_program_valid(self):
        """ Test valid job """
        jobId = self.jobIdDict["programValid"]
        self.passed = TestUtils.run_test(jobId,200,"finished",52,29,"complete",0,self)

    def test_program_mixed(self):
        """ Test mixed job with some rows failing """
        jobId = self.jobIdDict["programMixed"]
        self.passed = TestUtils.run_test(jobId,200,"finished",14016,12,"complete",121,self)

    def test_award_fin_valid(self):
        """ Test valid job """
        jobId = self.jobIdDict["awardFinValid"]
        self.passed = TestUtils.run_test(jobId,200,"finished",52,29,"complete",0,self)

    def test_award_fin_mixed(self):
        """ Test mixed job with some rows failing """
        jobId = self.jobIdDict["awardFinMixed"]
        self.passed = TestUtils.run_test(jobId,200,"finished",22571,15,"complete",178,self)

    def test_award_valid(self):
        """ Test valid job """
        jobId = self.jobIdDict["awardValid"]
        self.passed = TestUtils.run_test(jobId,200,"finished",52,29,"complete",0,self)

    def test_award_mixed(self):
        """ Test mixed job with some rows failing """
        jobId = self.jobIdDict["awardMixed"]
        self.passed = TestUtils.run_test(jobId,200,"finished",38706,14,"complete",384,self)


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