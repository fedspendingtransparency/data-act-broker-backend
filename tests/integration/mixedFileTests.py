from __future__ import print_function
from datetime import datetime
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.models.stagingModels import AwardFinancial
from tests.integration.baseTestValidator import BaseTestValidator
from tests.integration.fileTypeTests import FileTypeTests
import unittest

class MixedFileTests(BaseTestValidator):

    RULES_TO_APPLY = ('A1', 'A16', 'A18', 'A19', 'A2', 'A20', 'A21', 'A24', 'A3', 'A4', 'B11', 'B12', 'B13', 'B3', 'B4',
                      'B5', 'B6', 'B7', 'B9/B10', 'C14', 'C17', 'C18', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9')

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources."""
        super(MixedFileTests, cls).setUpClass()
        user = cls.userId
        force_tas_load = False

        print("Uploading files")
        # Upload needed files to S3
        s3FileNameMixed = cls.uploadFile("appropMixed.csv", user)
        s3FileNameProgramMixed = cls.uploadFile("programActivityMixed.csv", user)
        s3FileNameAwardFinMixed = cls.uploadFile("awardFinancialMixed.csv", user)
        s3FileNameAwardMixed = cls.uploadFile("awardMixed.csv", user)
        s3FileNameAwardMixedDelimiter = cls.uploadFile("awardMixedDelimiter.csv", user)
        s3FileNameCrossAwardFin = cls.uploadFile("cross_file_C.csv", user)
        s3FileNameCrossAward = cls.uploadFile("cross_file_D2.csv", user)
        s3FileNameCrossApprop = cls.uploadFile("cross_file_A.csv", user)
        s3FileNameCrossPgmAct = cls.uploadFile("cross_file_B.csv", user)
        s3FileNameProgramMixedShortcols = cls.uploadFile("programActivityMixedShortcols.csv", user)
        s3FileNameAwardFinMixedShortcols = cls.uploadFile("awardFinancialMixedShortcols.csv", user)
        s3FileNameAppropValidShortcols = cls.uploadFile("appropValidShortcols.csv", user)
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
        appropType = jobDb.getFileTypeId("appropriations")
        programType = jobDb.getFileTypeId("program_activity")
        awardFinType = jobDb.getFileTypeId("award_financial")
        awardType = jobDb.getFileTypeId("award")
        jobInfoList = {
            "mixed": [statusReady, jobTypeCsv, str(submissionIDs[2]), s3FileNameMixed, appropType],
            "programMixed": [statusReady, jobTypeCsv, str(submissionIDs[5]), s3FileNameProgramMixed, programType],
            "awardFinMixed": [statusReady, jobTypeCsv, str(submissionIDs[7]), s3FileNameAwardFinMixed, awardFinType],
            "awardMixed": [statusReady, jobTypeCsv, str(submissionIDs[9]), s3FileNameAwardMixed, awardType],
            "awardMixedDelimiter": [statusReady, jobTypeCsv, str(submissionIDs[10]), s3FileNameAwardMixedDelimiter, awardType],
            "crossApprop": [statusReady, jobTypeCsv, str(submissionIDs[11]), s3FileNameCrossApprop, appropType],
            "crossPgmAct": [statusReady, jobTypeCsv, str(submissionIDs[11]), s3FileNameCrossPgmAct, programType],
            "crossAwardFin": [statusReady, jobTypeCsv, str(submissionIDs[11]), s3FileNameCrossAwardFin, awardFinType],
            "crossAward": [statusReady, jobTypeCsv, str(submissionIDs[11]), s3FileNameCrossAward, awardType],
            "crossFile": [statusReady, jobTypeValidation, str(submissionIDs[11]), None, None],
            "appropValidShortcols": [statusReady, jobTypeCsv, str(submissionIDs[12]), s3FileNameAppropValidShortcols, appropType],
            "programMixedShortcols": [statusReady, jobTypeCsv, str(submissionIDs[13]), s3FileNameProgramMixedShortcols, programType],
            "awardFinMixedShortcols": [statusReady, jobTypeCsv, str(submissionIDs[14]), s3FileNameAwardFinMixedShortcols, awardFinType],
            "awardValidShortcols": [statusReady, jobTypeCsv, str(submissionIDs[15]), s3FileNameAwardValidShortcols, awardType]
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
        # TODO load only subset of rules
        FileTypeTests.load_definitions(cls.interfaces, force_tas_load, cls.RULES_TO_APPLY)

        cls.jobIdDict = jobIdDict

    def test_approp_valid_shortcol(self):
        """Test valid approp job with short colnames."""
        jobId = self.jobIdDict["appropValidShortcols"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0)

    def test_approp_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobIdDict["mixed"]
        self.passed = self.run_test(
            jobId, 200, "finished", 8212, 4, "complete", 39, 8, 841)

    def test_program_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobIdDict["programMixed"]
        self.passed = self.run_test(
        jobId, 200, "finished", 11390, 4, "complete", 81, 29, 10840)

    def test_program_mixed_shortcols(self):
        """Test object class/program activity job with some rows failing & short colnames."""
        jobId = self.jobIdDict["programMixedShortcols"]
        self.passed = self.run_test(
            jobId, 200, "finished", 11390, 4, "complete", 81, 29, 10840)

    def test_award_fin_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobIdDict["awardFinMixed"]
        self.passed = self.run_test(
        jobId, 200, "finished", 7537, 6, "complete", 47, 30, 9626)

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
            jobId, 200, "finished", 7537, 6, "complete", 47, 32, 11568)

    def test_award_valid_shortcols(self):
        """Test valid award (financial assistance) job with short colnames."""
        jobId = self.jobIdDict["awardValidShortcols"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0)

    def test_award_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobIdDict["awardMixed"]
        self.passed = self.run_test(
            jobId, 200, "finished", 123, 10, "complete", 1, 0, 63)

    def test_award_mixed_delimiter(self):
        """Test mixed job with mixed delimiter"""
        jobId = self.jobIdDict["awardMixedDelimiter"]
        self.passed = self.run_test(
            jobId, 400, "invalid", False, False, "header_error", 0)

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
            (2348, self.interfaces.errorDb.getCrossWarningReportName(submissionId, "appropriations", "program_activity")),
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