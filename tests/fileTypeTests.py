from __future__ import print_function
import os
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.models.validationModels import TASLookup
from dataactvalidator.filestreaming.schemaLoader import SchemaLoader
from dataactvalidator.scripts.loadTas import loadTas
from baseTestValidator import BaseTestValidator
import unittest

class FileTypeTests(BaseTestValidator):

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources."""
        super(FileTypeTests, cls).setUpClass()
        #TODO: refactor into a pytest fixture

        user = cls.userId
        # TODO: get rid of this flag once we're using a tempdb for test fixtures
        force_tas_load = False

        # Upload needed files to S3
        s3FileNameValid = cls.uploadFile("appropValid.csv", user)
        s3FileNameMixed = cls.uploadFile("appropMixed.csv", user)
        s3FileNameProgramValid = cls.uploadFile("programActivityValid.csv", user)
        s3FileNameProgramMixed = cls.uploadFile("programActivityMixed.csv", user)
        s3FileNameAwardFinValid = cls.uploadFile("awardFinancialValid.csv", user)
        s3FileNameAwardFinMixed = cls.uploadFile("awardFinancialMixed.csv", user)
        s3FileNameAwardValid = cls.uploadFile("awardValid.csv", user)
        s3FileNameAwardMixed = cls.uploadFile("awardMixed.csv", user)
        s3FileNameAwardMixedDelimiter = cls.uploadFile("awardMixedDelimiter.csv", user)
        s3FileNameCrossAwardFin = cls.uploadFile("cross_file_C.csv", user)
        s3FileNameCrossAward = cls.uploadFile("cross_file_D2.csv", user)

        # Create submissions and get IDs back
        submissionIDs = {}
        for i in range(0, 12):
            submissionIDs[i] = cls.insertSubmission(cls.jobTracker, user)

        # Create jobs
        jobDb = cls.jobTracker
        jobInfoList = {
            "valid": [str(jobDb.getJobStatusId("ready")), str(jobDb.getJobTypeId("csv_record_validation")), str(submissionIDs[1]), s3FileNameValid, 3],
            "mixed": [str(jobDb.getJobStatusId("ready")), str(jobDb.getJobTypeId("csv_record_validation")), str(submissionIDs[2]), s3FileNameMixed, 3],
            "programValid": [str(jobDb.getJobStatusId("ready")), str(jobDb.getJobTypeId("csv_record_validation")), str(submissionIDs[4]), s3FileNameProgramValid, 4],
            "programMixed": [str(jobDb.getJobStatusId("ready")), str(jobDb.getJobTypeId("csv_record_validation")), str(submissionIDs[5]), s3FileNameProgramMixed, 4],
            "awardFinValid": [str(jobDb.getJobStatusId("ready")), str(jobDb.getJobTypeId("csv_record_validation")), str(submissionIDs[6]), s3FileNameAwardFinValid, 2],
            "awardFinMixed": [str(jobDb.getJobStatusId("ready")), str(jobDb.getJobTypeId("csv_record_validation")), str(submissionIDs[7]), s3FileNameAwardFinMixed, 2],
            "awardValid": [str(jobDb.getJobStatusId("ready")), str(jobDb.getJobTypeId("csv_record_validation")), str(submissionIDs[8]), s3FileNameAwardValid, 1],
            "awardMixed": [str(jobDb.getJobStatusId("ready")), str(jobDb.getJobTypeId("csv_record_validation")), str(submissionIDs[9]), s3FileNameAwardMixed, 1],
            "awardMixedDelimiter": [str(jobDb.getJobStatusId("ready")), str(jobDb.getJobTypeId("csv_record_validation")), str(submissionIDs[10]), s3FileNameAwardMixedDelimiter, 1],
            "crossAwardFin": [str(jobDb.getJobStatusId("ready")), str(jobDb.getJobTypeId("csv_record_validation")), str(submissionIDs[11]), s3FileNameCrossAwardFin, 2],
            "crossAward": [str(jobDb.getJobStatusId("ready")), str(jobDb.getJobTypeId("csv_record_validation")), str(submissionIDs[11]), s3FileNameCrossAward, 1],
            "crossFile": [str(jobDb.getJobStatusId("ready")), str(jobDb.getJobTypeId("validation")), str(submissionIDs[11]), None, None]
        }

        jobIdDict = {}
        for key in jobInfoList:
            jobInfo = jobInfoList[key]  # Done this way to be compatible with python 2 and 3
            jobInfo.append(jobDb.session)
            job = cls.addJob(*jobInfo)
            jobId = job.job_id
            jobIdDict[key] = jobId
            print("".join([str(key),": ",str(cls.jobTracker.getSubmissionId(jobId)), ", "]), end = "")

        # Load fields and rules
        FileTypeTests.load_definitions(cls.interfaces, force_tas_load)

        cls.jobIdDict = jobIdDict

    @staticmethod
    def load_definitions(interfaces, force_tas_load):
        """Load file definitions."""
        # TODO: introduce flexibility re: test file location
        SchemaLoader.loadFields("appropriations","../dataactvalidator/config/appropFields.csv")
        SchemaLoader.loadFields("program_activity","../dataactvalidator/config/programActivityFields.csv")
        SchemaLoader.loadFields("award_financial","../dataactvalidator/config/awardFinancialFields.csv")
        SchemaLoader.loadFields("award","../dataactvalidator/config/awardFields.csv")
        SchemaLoader.loadRules("appropriations","../dataactvalidator/config/appropRules.csv")
        SchemaLoader.loadRules("program_activity","../dataactvalidator/config/programActivityRules.csv")
        SchemaLoader.loadRules("award_financial","../dataactvalidator/config/awardFinancialRules.csv")
        SchemaLoader.loadCrossRules("../dataactvalidator/config/crossFileRules.csv")
        if (interfaces.validationDb.session.query(TASLookup).count() == 0
                or force_tas_load):
            # TAS table is empty, load it
            loadTas(tasFile="all_tas_betc.csv", dropIdx=False)

    def test_approp_valid(self):
        """Test valid job."""
        jobId = self.jobIdDict["valid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 52, 10, "complete", 0, False)

    def test_approp_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobIdDict["mixed"]
        self.passed = self.run_test(
            jobId, 200, "finished", 4110, 4, "complete", 40, True)

    def test_program_valid(self):
        """Test valid job."""
        jobId = self.jobIdDict["programValid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 52, 10, "complete", 0, False)

    def test_program_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobIdDict["programMixed"]
        self.passed = self.run_test(
            jobId, 200, "finished", 9247, 4, "complete", 88, True)

    def test_award_fin_valid(self):
        """Test valid job."""
        jobId = self.jobIdDict["awardFinValid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 52, 11, "complete", 0, False)

    def test_award_fin_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobIdDict["awardFinMixed"]
        self.passed = self.run_test(
            jobId, 200, "finished", 5808, 6, "complete", 53, True)

    def test_award_valid(self):
        """Test valid job."""
        jobId = self.jobIdDict["awardValid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 52, 10, "complete", 0, False)

    def test_award_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobIdDict["awardMixed"]
        self.passed = self.run_test(
            jobId, 200, "finished", 3194, 5, "complete", 40, True)

    def test_award_mixed_delimiter(self):
        """Test mixed job with mixed delimiter"""
        jobId = self.jobIdDict["awardMixedDelimiter"]
        self.passed = self.run_test(
            jobId, 400, "invalid", False, False, "header_error", 0, False)

    def test_cross_file(self):
        crossId = self.jobIdDict["crossFile"]
        # Run jobs for C and D2, then cross file validation job
        awardFinResponse = self.validateJob(self.jobIdDict["crossAwardFin"],self.useThreads)
        self.assertEqual(awardFinResponse.status_code, 200,msg=str(awardFinResponse.json))
        awardResponse = self.validateJob(self.jobIdDict["crossAward"],self.useThreads)
        self.assertEqual(awardResponse.status_code, 200,msg=str(awardResponse.json))
        crossFileResponse = self.validateJob(crossId,self.useThreads)
        self.assertEqual(crossFileResponse.status_code, 200,msg=str(crossFileResponse.json))
        # Check number of cross file validation errors in DB for this job
        self.assertEqual(self.interfaces.errorDb.checkNumberOfErrorsByJobId(crossId),2)
        # Check cross file job complete
        self.waitOnJob(self.interfaces.jobDb, crossId, "finished", self.useThreads)
        # Check that cross file validation report exists and is the right size
        jobTracker = self.interfaces.jobDb
        fileSize = 410
        reportPath = jobTracker.getCrossFileReportPath(jobTracker.getSubmissionId(crossId))
        if self.local:
            path = "".join(
                [self.local_file_directory,reportPath])
            self.assertGreater(os.path.getsize(path), fileSize - 5)
            self.assertLess(os.path.getsize(path), fileSize + 5)
        else:
            self.assertGreater(s3UrlHandler.getFileSize(
                "errors/"+reportPath), fileSize - 5)
            self.assertLess(s3UrlHandler.getFileSize(
                "errors/"+reportPath), fileSize + 5)

if __name__ == '__main__':
    unittest.main()