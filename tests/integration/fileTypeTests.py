from __future__ import print_function
import os
from os.path import join
from datetime import datetime
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.models.domainModels import TASLookup
from dataactcore.models.stagingModels import AwardFinancial
from dataactcore.config import CONFIG_BROKER
from dataactvalidator.filestreaming.sqlLoader import SQLLoader
from dataactvalidator.filestreaming.schemaLoader import SchemaLoader
from dataactvalidator.filestreaming.loadFile import loadDomainValues
from dataactvalidator.scripts.loadTas import loadTas
from tests.integration.baseTestValidator import BaseTestValidator
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

        print("Uploading files")
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
        s3FileNameCrossApprop = cls.uploadFile("cross_file_A.csv", user)
        s3FileNameCrossPgmAct = cls.uploadFile("cross_file_B.csv", user)
        s3FileNameAppropValidShortcols = cls.uploadFile("appropValidShortcols.csv", user)
        s3FileNameProgramMixedShortcols = cls.uploadFile("programActivityMixedShortcols.csv", user)
        s3FileNameAwardFinMixedShortcols = cls.uploadFile("awardFinancialMixedShortcols.csv", user)
        s3FileNameAwardValidShortcols = cls.uploadFile("awardValidShortcols.csv", user)

        # Create submissions and get IDs back
        submissionIDs = {}
        for i in range(0, 16):
            if i == 7:
               # Award financial mixed will be second quarter
                submissionIDs[i] = cls.insertSubmission(cls.jobTracker, user, datetime(2015,3,15))
            else:
                submissionIDs[i] = cls.insertSubmission(cls.jobTracker, user)


        # Create jobs
        jobDb = cls.jobTracker
        statusReady = str(jobDb.getJobStatusId("ready"))
        jobTypeCsv = str(jobDb.getJobTypeId("csv_record_validation"))
        jobTypeValidation = str(jobDb.getJobTypeId("validation"))
        jobInfoList = {
            "valid": [statusReady, jobTypeCsv, str(submissionIDs[1]), s3FileNameValid, 1],
            "mixed": [statusReady, jobTypeCsv, str(submissionIDs[2]), s3FileNameMixed, 1],
            "programValid": [statusReady, jobTypeCsv, str(submissionIDs[4]), s3FileNameProgramValid, 2],
            "programMixed": [statusReady, jobTypeCsv, str(submissionIDs[5]), s3FileNameProgramMixed, 2],
            "awardFinValid": [statusReady, jobTypeCsv, str(submissionIDs[6]), s3FileNameAwardFinValid, 3],
            "awardFinMixed": [statusReady, jobTypeCsv, str(submissionIDs[7]), s3FileNameAwardFinMixed, 3],
            "awardValid": [statusReady, jobTypeCsv, str(submissionIDs[8]), s3FileNameAwardValid, 4],
            "awardMixed": [statusReady, jobTypeCsv, str(submissionIDs[9]), s3FileNameAwardMixed, 4],
            "awardMixedDelimiter": [statusReady, jobTypeCsv, str(submissionIDs[10]), s3FileNameAwardMixedDelimiter, 4],
            "crossApprop": [statusReady, jobTypeCsv, str(submissionIDs[11]), s3FileNameCrossApprop, 1],
            "crossPgmAct": [statusReady, jobTypeCsv, str(submissionIDs[11]), s3FileNameCrossPgmAct, 2],
            "crossAwardFin": [statusReady, jobTypeCsv, str(submissionIDs[11]), s3FileNameCrossAwardFin, 3],
            "crossAward": [statusReady, jobTypeCsv, str(submissionIDs[11]), s3FileNameCrossAward, 4],
            "crossFile": [statusReady, jobTypeValidation, str(submissionIDs[11]), None, None],
            "appropValidShortcols": [statusReady, jobTypeCsv, str(submissionIDs[12]), s3FileNameAppropValidShortcols, 1],
            "programMixedShortcols": [statusReady, jobTypeCsv, str(submissionIDs[13]), s3FileNameProgramMixedShortcols, 2],
            "awardFinMixedShortcols": [statusReady, jobTypeCsv, str(submissionIDs[14]), s3FileNameAwardFinMixedShortcols, 3],
            "awardValidShortcols": [statusReady, jobTypeCsv, str(submissionIDs[15]), s3FileNameAwardValidShortcols, 4]
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
        SchemaLoader.loadAllFromPath(join(CONFIG_BROKER["path"],"dataactvalidator","config"))
        SQLLoader.loadSql("sqlRules.csv")
        # Load domain values tables
        loadDomainValues(
            join(CONFIG_BROKER["path"], "dataactvalidator", "config"),
            join(CONFIG_BROKER["path"], os.path.join("tests", "integration", "data"), "sf_133.csv"),
            join(CONFIG_BROKER["path"], os.path.join("tests", "integration", "data"), "program_activity.csv"))
        if (interfaces.validationDb.session.query(TASLookup).count() == 0
                or force_tas_load):
            # TAS table is empty, load it
            loadTas(tasFile=os.path.join(CONFIG_BROKER["path"], "tests", "integration", "data", "all_tas_betc.csv"), dropIdx=False)

    def test_approp_valid(self):
        """Test valid job."""
        jobId = self.jobIdDict["valid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0, False)

    def test_approp_valid_shortcol(self):
        """Test valid approp job with short colnames."""
        jobId = self.jobIdDict["appropValidShortcols"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0, False)

    def test_approp_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobIdDict["mixed"]
        self.passed = self.run_test(
            jobId, 200, "finished", 8212, 4, "complete", 39, True, 8, 841)


    def test_program_valid(self):
        """Test valid job."""
        jobId = self.jobIdDict["programValid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0, False)

    def test_program_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobIdDict["programMixed"]
        self.passed = self.run_test(
        jobId, 200, "finished", 12058, 4, "complete", 84, True, 27, 10510)

    def test_program_mixed_shortcols(self):
        """Test object class/program activity job with some rows failing & short colnames."""
        jobId = self.jobIdDict["programMixedShortcols"]
        self.passed = self.run_test(
            jobId, 200, "finished", 12058, 4, "complete", 84, True, 27, 10510)

    def test_award_fin_valid(self):
        """Test valid job."""
        jobId = self.jobIdDict["awardFinValid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 11, "complete", 0, False)

    def test_award_fin_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobIdDict["awardFinMixed"]
        self.passed = self.run_test(
        jobId, 200, "finished", 8000, 7, "complete", 48, True, 29, 9127)

        # Test that whitespace is converted to null
        rowThree = self.interfaces.validationDb.session.query(AwardFinancial).filter(AwardFinancial.parent_award_id == "ZZZZ").filter(AwardFinancial.submission_id == self.interfaces.jobDb.getSubmissionId(jobId)).first()
        self.assertIsNone(rowThree.agency_identifier)
        self.assertIsNone(rowThree.piid)
        # And commas removed for numeric
        rowThirteen = self.interfaces.validationDb.session.query(AwardFinancial).filter(AwardFinancial.parent_award_id == "YYYY").filter(AwardFinancial.submission_id == self.interfaces.jobDb.getSubmissionId(jobId)).first()
        self.assertEqual(rowThirteen.deobligations_recov_by_awa_cpe,26000)

    def test_award_fin_mixed_shortcols(self):
        """Test award financial job with some rows failing & short colnames."""
        jobId = self.jobIdDict["awardFinMixedShortcols"]
        self.passed = self.run_test(
            jobId, 200, "finished", 8000, 7, "complete", 48, True, 31, 11069)

    def test_award_valid(self):
        """Test valid job."""
        jobId = self.jobIdDict["awardValid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0, False)

    def test_award_valid_shortcols(self):
        """Test valid award (financial assistance) job with short colnames."""
        jobId = self.jobIdDict["awardValidShortcols"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0, False)

    def test_award_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobIdDict["awardMixed"]
        self.passed = self.run_test(
            jobId, 200, "finished", 1139, 8, "complete", 17, True, 24, 2085)

    def test_award_mixed_delimiter(self):
        """Test mixed job with mixed delimiter"""
        jobId = self.jobIdDict["awardMixedDelimiter"]
        self.passed = self.run_test(
            jobId, 400, "invalid", False, False, "header_error", 0, False)

    def test_cross_file(self):
        crossId = self.jobIdDict["crossFile"]
        # Run jobs for A, B, C, and D2, then cross file validation job
        # Note: test files used for cross validation use the short column names
        # as a way to ensure those are handled correctly by the validator
        awardFinResponse = self.validateJob(self.jobIdDict["crossAwardFin"],self.useThreads)
        self.assertEqual(awardFinResponse.status_code, 200, msg=str(awardFinResponse.json))
        awardResponse = self.validateJob(self.jobIdDict["crossAward"],self.useThreads)
        self.assertEqual(awardResponse.status_code, 200, msg=str(awardResponse.json))
        appropResponse = self.validateJob(self.jobIdDict["crossApprop"], self.useThreads)
        self.assertEqual(appropResponse.status_code, 200, msg=str(appropResponse.json))
        pgmActResponse = self.validateJob(self.jobIdDict["crossPgmAct"], self.useThreads)
        self.assertEqual(pgmActResponse.status_code, 200, msg=str(pgmActResponse.json))
        crossFileResponse = self.validateJob(crossId, self.useThreads)
        self.assertEqual(crossFileResponse.status_code, 200, msg=str(crossFileResponse.json))

        # Check number of cross file validation errors in DB for this job
        self.assertEqual(self.interfaces.errorDb.checkNumberOfErrorsByJobId(crossId, self.interfaces.validationDb, "fatal"), 0)
        self.assertEqual(self.interfaces.errorDb.checkNumberOfErrorsByJobId(crossId, self.interfaces.validationDb, "warning"), 5)
        # Check cross file job complete
        self.waitOnJob(self.interfaces.jobDb, crossId, "finished", self.useThreads)
        # Check that cross file validation report exists and is the right size
        jobTracker = self.interfaces.jobDb

        submissionId = jobTracker.getSubmissionId(crossId)
        sizePathPairs = [
            (89, self.interfaces.errorDb.getCrossReportName(submissionId, "appropriations", "program_activity")),
            (89, self.interfaces.errorDb.getCrossReportName(submissionId, "award_financial", "award")),
            (1329, self.interfaces.errorDb.getCrossWarningReportName(submissionId, "appropriations", "program_activity")),
            (424, self.interfaces.errorDb.getCrossWarningReportName(submissionId, "award_financial", "award")),
        ]

        for size, path in sizePathPairs:
            if self.local:
                self.assertFileSizeAppxy(size, path)
            else:
                self.assertGreater(
                    s3UrlHandler.getFileSize("errors/" + path), size - 5)
                self.assertLess(
                    s3UrlHandler.getFileSize("errors/" + path), size + 5)

if __name__ == '__main__':
    unittest.main()
