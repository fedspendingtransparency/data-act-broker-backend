from sqlalchemy.orm.exc import NoResultFound

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import JobDependency, Job
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT, FIELD_TYPE_DICT
from dataactcore.models.validationModels import FileColumn
from dataactvalidator.app import create_app
from tests.integration.baseTestValidator import BaseTestValidator


class JobTests(BaseTestValidator):

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources (test data)"""
        super(JobTests, cls).setUpClass()
        user = cls.userId

        # Flag for testing a million+ errors (can take ~30 min to run)
        cls.includeLongTests = False

        with create_app().app_context():
            # get the submission test user
            sess = GlobalDB.db().session

            # Create test submissions and jobs, also uploading
            # the files needed for each job.
            job_dict = {}

            submission_id = cls.insert_submission(sess, user)
            job_info = Job(
                job_status_id=JOB_STATUS_DICT['ready'],
                job_type_id=JOB_TYPE_DICT['file_upload'],
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['bad_upload'] = job_info.job_id

            submission_id = cls.insert_submission(sess, user)
            job_info = Job(
                job_status_id=JOB_STATUS_DICT['ready'],
                job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['bad_prereq'] = job_info.job_id

            submission_id = cls.insert_submission(sess, user)
            job_info = Job(
                job_status_id=JOB_STATUS_DICT['ready'],
                job_type_id=JOB_TYPE_DICT['external_validation'],
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['wrong_type'] = job_info.job_id

            submission_id = cls.insert_submission(sess, user)
            job_info = Job(
                job_status_id=JOB_STATUS_DICT['finished'],
                job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['not_ready'] = job_info.job_id

            submission_id = cls.insert_submission(sess, user)
            job_info = Job(
                filename=cls.upload_file('testEmpty.csv', user),
                job_status_id=JOB_STATUS_DICT['ready'],
                job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['empty'] = job_info.job_id

            # create dependency
            dependency = JobDependency(job_id=job_dict["bad_prereq"], prerequisite_id=job_dict["bad_upload"])
            sess.add(dependency)

            col_id_dict = {}
            for file_id in range(1, 5):
                for column_id in range(1, 6):
                    if column_id < 3:
                        field_type = FIELD_TYPE_DICT['INT']
                    else:
                        field_type = FIELD_TYPE_DICT['STRING']
                    column_name = "header_{}".format(column_id)

                    file_col = FileColumn(
                        file_id=file_id,
                        field_types_id=field_type,
                        name=column_name,
                        required=(column_id != FIELD_TYPE_DICT['STRING']))
                    sess.add(file_col)
                    sess.flush()
                    col_id_dict["header_{}_file_type_{}".format(column_id, file_id)] = file_col.file_column_id

            # commit submissions/jobs and output IDs
            sess.commit()
            for job_type, job_id in job_dict.items():
                print('{}: {}'.format(job_type, job_id))

            cls.jobDict = job_dict

    def tearDown(self):
        super(JobTests, self).tearDown()

    def test_empty(self):
        """Test empty file."""
        job_id = self.jobDict["empty"]
        response = self.run_test(job_id, 400, "invalid", False, False, "single_row_error", 0)

        self.assertEqual(response.json["message"], "CSV file must have a header")

    def test_bad_id_job(self):
        """Test job ID not found in job table."""
        job_id = -1
        with self.assertRaises(NoResultFound):
            self.run_test(job_id, 400, False, False, False, False, 0)

    def test_bad_prereq_job(self):
        """Test job with unfinished prerequisites."""
        job_id = self.jobDict["bad_prereq"]
        self.run_test(job_id, 400, "ready", False, False, "job_error", 0)

    def test_bad_type_job(self):
        """Test job with wrong type."""
        job_id = self.jobDict["wrong_type"]
        self.run_test(job_id, 400, "ready", False, False, "job_error", 0)
