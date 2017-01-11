import os

from sqlalchemy import not_

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import TASLookup
from dataactcore.models.jobModels import Job
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.models.validationModels import RuleSql
from dataactcore.config import CONFIG_BROKER
from dataactvalidator.app import create_app
from dataactvalidator.filestreaming.sqlLoader import SQLLoader
from dataactvalidator.filestreaming.schemaLoader import SchemaLoader
from dataactvalidator.scripts.loadFile import load_domain_values
from dataactvalidator.scripts.loadTas import load_tas
from dataactvalidator.scripts.load_sf133 import load_all_sf133
from tests.integration.baseTestValidator import BaseTestValidator


class FileTypeTests(BaseTestValidator):

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources."""
        super(FileTypeTests, cls).setUpClass()
        # TODO: refactor into a pytest fixture

        user = cls.userId
        # TODO: get rid of this flag once we're using a tempdb for test fixtures
        force_tas_load = False

        with create_app().app_context():
            sess = GlobalDB.db().session

            # Create submissions and jobs, also uploading
            # the files needed for each job.
            status_ready_id = JOB_STATUS_DICT['ready']
            job_type_csv_id = JOB_TYPE_DICT['csv_record_validation']
            job_dict = {}

            submission_id = cls.insert_submission(sess, user)
            job_info = Job(
                filename=cls.upload_file("appropValid.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['valid'] = job_info.job_id

            submission_id = cls.insert_submission(sess, user)
            job_info = Job(
                filename=cls.upload_file("programActivityValid.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['program_activity'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['programValid'] = job_info.job_id

            submission_id = cls.insert_submission(sess, user)
            job_info = Job(
                filename=cls.upload_file("awardFinancialValid.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['award_financial'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['awardFinValid'] = job_info.job_id

            # next two jobs have the same submission id
            submission_id = cls.insert_submission(sess, user)
            job_info = Job(
                filename=cls.upload_file("awardValid.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['award'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['awardValid'] = job_info.job_id

            job_info = Job(
                filename=cls.upload_file("awardProcValid.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['award_procurement'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['awardProcValid'] = job_info.job_id

            # commit submissions/jobs and output IDs
            sess.commit()
            for job_type, job_id in job_dict.items():
                print('{}: {}'.format(job_type, job_id))

            # Load fields and rules
            FileTypeTests.load_definitions(sess, force_tas_load)

            cls.jobDict = job_dict

    @staticmethod
    def load_definitions(sess, force_tas_load, rule_list=None):
        """Load file definitions."""
        validator_config_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config")
        integration_test_data_path = os.path.join(CONFIG_BROKER["path"], "tests", "integration", "data")

        SchemaLoader.load_all_from_path(validator_config_path)
        SQLLoader.load_sql("sqlRules.csv")

        if rule_list is not None:
            # If rule list provided, drop all other rules
            sess.query(RuleSql).filter(not_(
                RuleSql.rule_label.in_(rule_list))).delete(synchronize_session='fetch')
            sess.commit()

        # Load domain values tables
        load_domain_values(
            validator_config_path,
            os.path.join(integration_test_data_path, "program_activity.csv"))
        if sess.query(TASLookup).count() == 0 or force_tas_load:
            # TAS table is empty, load it
            load_tas(tas_file=os.path.join(integration_test_data_path, "cars_tas.csv"))

        # Load test SF-133
        load_all_sf133(integration_test_data_path)

    def test_approp_valid(self):
        """Test valid job."""
        job_id = self.jobDict["valid"]
        self.passed = self.run_test(job_id, 200, "finished", 63, 10, "complete", 0, num_warnings=10)

    def test_program_valid(self):
        """Test valid job."""
        job_id = self.jobDict["programValid"]
        self.passed = self.run_test(job_id, 200, "finished", 63, 10, "complete", 0)

    def test_award_fin_valid(self):
        """Test valid job."""
        job_id = self.jobDict["awardFinValid"]
        self.passed = self.run_test(job_id, 200, "finished", 63, 10, "complete", 0, num_warnings=3)

    def test_award_valid(self):
        """Test valid job."""
        job_id = self.jobDict["awardValid"]
        self.passed = self.run_test(job_id, 200, "finished", 63, 10, "complete", 0)

    def test_award_proc_valid(self):
        """Test valid job."""
        job_id = self.jobDict["awardProcValid"]
        self.passed = self.run_test(job_id, 200, "finished", 63, 10, "complete", 0, False)
