from __future__ import print_function
import os
from sqlalchemy import not_
from datetime import datetime
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.models.domainModels import TASLookup
from dataactcore.models.stagingModels import AwardFinancial
from dataactcore.models.validationModels import RuleSql
from dataactcore.config import CONFIG_BROKER
from dataactvalidator.filestreaming.sqlLoader import SQLLoader
from dataactvalidator.filestreaming.schemaLoader import SchemaLoader
from dataactvalidator.scripts.loadFile import loadDomainValues
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
        s3FileNameProgramValid = cls.uploadFile("programActivityValid.csv", user)
        s3FileNameAwardFinValid = cls.uploadFile("awardFinancialValid.csv", user)
        s3FileNameAwardValid = cls.uploadFile("awardValid.csv", user)
        s3FileNameAwardProcValid = cls.uploadFile("awardProcValid.csv", user)

        # Create submissions and get IDs back
        submissionIDs = {}
        for i in range(0, 9):
            submissionIDs[i] = cls.insertSubmission(cls.jobTracker, user)

        # Create jobs
        jobDb = cls.jobTracker
        statusReady = str(jobDb.getJobStatusId("ready"))
        jobTypeCsv = str(jobDb.getJobTypeId("csv_record_validation"))
        jobInfoList = {
            "valid": [statusReady, jobTypeCsv, str(submissionIDs[1]), s3FileNameValid, jobDb.getFileTypeId("appropriations")],
            "programValid": [statusReady, jobTypeCsv, str(submissionIDs[4]), s3FileNameProgramValid, jobDb.getFileTypeId("program_activity")],
            "awardFinValid": [statusReady, jobTypeCsv, str(submissionIDs[6]), s3FileNameAwardFinValid, jobDb.getFileTypeId("award_financial")],
            "awardValid": [statusReady, jobTypeCsv, str(submissionIDs[8]), s3FileNameAwardValid, jobDb.getFileTypeId("award")],
            "awardProcValid": [statusReady, jobTypeCsv, str(submissionIDs[8]), s3FileNameAwardProcValid, jobDb.getFileTypeId("award_procurement")]
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
    def load_definitions(interfaces, force_tas_load, ruleList = None):
        """Load file definitions."""
        SchemaLoader.loadAllFromPath(os.path.join(CONFIG_BROKER["path"],"dataactvalidator","config"))
        SQLLoader.loadSql("sqlRules.csv")

        if ruleList is not None:
            # If rule list provided, drop all other rules
            to_delete = interfaces.validationDb.session.query(RuleSql).filter(not_(
                RuleSql.rule_label.in_(ruleList)))
            for rule in to_delete:
                interfaces.validationDb.session.delete(rule)
            interfaces.validationDb.session.commit()

        # Load domain values tables
        loadDomainValues(
            os.path.join(CONFIG_BROKER["path"],"dataactvalidator","config"),
            os.path.join(CONFIG_BROKER["path"], "tests", "integration", "data"),
            os.path.join(CONFIG_BROKER["path"], "tests", "integration", "data", "program_activity.csv"))
        if (interfaces.validationDb.session.query(TASLookup).count() == 0
                or force_tas_load):
            # TAS table is empty, load it
            loadTas(tasFile=os.path.join(CONFIG_BROKER["path"], "tests", "integration", "data", "all_tas_betc.csv"))

    def test_approp_valid(self):
        """Test valid job."""
        jobId = self.jobIdDict["valid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0, numWarnings=10)

    def test_program_valid(self):
        """Test valid job."""
        jobId = self.jobIdDict["programValid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0)

    def test_award_fin_valid(self):
        """Test valid job."""
        jobId = self.jobIdDict["awardFinValid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0)

    def test_award_valid(self):
        """Test valid job."""
        jobId = self.jobIdDict["awardValid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0)

    def test_award_proc_valid(self):
        """Test valid job."""
        jobId = self.jobIdDict["awardProcValid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0, False)


if __name__ == '__main__':
    unittest.main()
