from __future__ import print_function
import os
import unittest

from sqlalchemy import not_

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import TASLookup
from dataactcore.models.jobModels import Job
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.models.validationModels import RuleSql
from dataactcore.config import CONFIG_BROKER
from dataactvalidator.app import createApp
from dataactvalidator.filestreaming.sqlLoader import SQLLoader
from dataactvalidator.filestreaming.schemaLoader import SchemaLoader
from dataactvalidator.scripts.loadFile import loadDomainValues
from dataactvalidator.scripts.loadTas import loadTas
from dataactvalidator.scripts.load_sf133 import load_all_sf133
from tests.integration.baseTestValidator import BaseTestValidator


class FileTypeTests(BaseTestValidator):

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources."""
        super(FileTypeTests, cls).setUpClass()
        #TODO: refactor into a pytest fixture

        user = cls.userId
        # TODO: get rid of this flag once we're using a tempdb for test fixtures
        force_tas_load = False

        with createApp().app_context():
            sess = GlobalDB.db().session

            # Create submissions and jobs, also uploading
            # the files needed for each job.
            statusReadyId = JOB_STATUS_DICT['ready']
            jobTypeCsvId = JOB_TYPE_DICT['csv_record_validation']
            jobDict = {}

            submissionId = cls.insertSubmission(sess, user)
            job_info = Job(
                filename=cls.uploadFile("appropValid.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['valid'] = job_info.job_id

            submissionId = cls.insertSubmission(sess, user)
            job_info = Job(
                filename=cls.uploadFile("programActivityValid.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['program_activity'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['programValid'] = job_info.job_id

            submissionId = cls.insertSubmission(sess, user)
            job_info = Job(
                filename=cls.uploadFile("awardFinancialValid.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['award_financial'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['awardFinValid'] = job_info.job_id

            # next two jobs have the same submission id
            submissionId = cls.insertSubmission(sess, user)
            job_info = Job(
                filename=cls.uploadFile("awardValid.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['award'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['awardValid'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("awardProcValid.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['award_procurement'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['awardProcValid'] = job_info.job_id

            # commit submissions/jobs and output IDs
            sess.commit()
            for job_type, job_id in jobDict.items():
                print('{}: {}'.format(job_type, job_id))

            # Load fields and rules
            FileTypeTests.load_definitions(sess, force_tas_load)

            cls.jobDict = jobDict

    @staticmethod
    def load_definitions(sess, force_tas_load, ruleList=None):
        """Load file definitions."""
        validator_config_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config")
        integration_test_data_path = os.path.join(CONFIG_BROKER["path"], "tests", "integration", "data")

        SchemaLoader.loadAllFromPath(validator_config_path)
        SQLLoader.loadSql("sqlRules.csv")

        if ruleList is not None:
            # If rule list provided, drop all other rules
            sess.query(RuleSql).filter(not_(
                RuleSql.rule_label.in_(ruleList))).delete(synchronize_session='fetch')
            sess.commit()

        # Load domain values tables
        loadDomainValues(
            validator_config_path,
            os.path.join(integration_test_data_path, "program_activity.csv"))
        if sess.query(TASLookup).count() == 0 or force_tas_load:
            # TAS table is empty, load it
            loadTas(tasFile=os.path.join(integration_test_data_path, "cars_tas.csv"))

        # Load test SF-133
        load_all_sf133(integration_test_data_path)

    def test_approp_valid(self):
        """Test valid job."""
        jobId = self.jobDict["valid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0, numWarnings=10)

    def test_program_valid(self):
        """Test valid job."""
        jobId = self.jobDict["programValid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0)

    def test_award_fin_valid(self):
        """Test valid job."""
        jobId = self.jobDict["awardFinValid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0, numWarnings=3)

    def test_award_valid(self):
        """Test valid job."""
        jobId = self.jobDict["awardValid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0)

    def test_award_proc_valid(self):
        """Test valid job."""
        jobId = self.jobDict["awardProcValid"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0, False)


if __name__ == '__main__':
    unittest.main()
