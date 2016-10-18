from __future__ import print_function
import unittest

from sqlalchemy.orm.exc import NoResultFound

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import JobDependency, Job
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT, FIELD_TYPE_DICT
from dataactcore.models.validationModels import FileColumn
from dataactvalidator.app import createApp
from tests.integration.baseTestValidator import BaseTestValidator


class JobTests(BaseTestValidator):

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources (test data)"""
        super(JobTests, cls).setUpClass()
        user = cls.userId

        # Flag for testing a million+ errors (can take ~30 min to run)
        cls.includeLongTests = False

        with createApp().app_context():
            # get the submission test user
            sess = GlobalDB.db().session

            # Create test submissions and jobs, also uploading
            # the files needed for each job.
            jobDict = {}

            submissionId = cls.insertSubmission(sess, user)
            job_info = Job(
                job_status_id=JOB_STATUS_DICT['ready'],
                job_type_id=JOB_TYPE_DICT['file_upload'],
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['bad_upload'] = job_info.job_id

            submissionId = cls.insertSubmission(sess, user)
            job_info = Job(
                job_status_id=JOB_STATUS_DICT['ready'],
                job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['bad_prereq'] = job_info.job_id

            submissionId = cls.insertSubmission(sess, user)
            job_info = Job(
                job_status_id=JOB_STATUS_DICT['ready'],
                job_type_id=JOB_TYPE_DICT['external_validation'],
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['wrong_type'] = job_info.job_id

            submissionId = cls.insertSubmission(sess, user)
            job_info = Job(
                job_status_id=JOB_STATUS_DICT['finished'],
                job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['not_ready'] = job_info.job_id

            submissionId = cls.insertSubmission(sess, user)
            job_info = Job(
                filename=cls.uploadFile('testEmpty.csv', user),
                job_status_id=JOB_STATUS_DICT['ready'],
                job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['empty'] = job_info.job_id

            # create dependency
            dependency = JobDependency(
                job_id=jobDict["bad_prereq"],
                prerequisite_id=jobDict["bad_upload"])
            sess.add(dependency)

            colIdDict = {}
            for fileId in range(1, 5):
                for columnId in range(1, 6):
                    if columnId < 3:
                        fieldType = FIELD_TYPE_DICT['INT']
                    else:
                        fieldType = FIELD_TYPE_DICT['STRING']
                    columnName = "header_{}".format(columnId)

                    fileCol = FileColumn(
                        file_id=fileId,
                        field_types_id=fieldType,
                        name=columnName,
                        required=(columnId != FIELD_TYPE_DICT['STRING']))
                    sess.add(fileCol)
                    sess.flush()
                    colIdDict["header_{}_file_type_{}".format(
                        columnId, fileId)] = fileCol.file_column_id

            # commit submissions/jobs and output IDs
            sess.commit()
            for job_type, job_id in jobDict.items():
                print('{}: {}'.format(job_type, job_id))

            cls.jobDict = jobDict

    def tearDown(self):
        super(JobTests, self).tearDown()

    def test_empty(self):
        """Test empty file."""
        jobId = self.jobDict["empty"]
        response = self.run_test(
            jobId, 400, "invalid", False, False, "single_row_error", 0)

        self.assertEqual(response.json["message"], "CSV file must have a header")

    def test_bad_id_job(self):
        """Test job ID not found in job table."""
        # This test is in an in-between place as we refactor database access.
        # Because run_test now retrieves a job directly from the db instead of
        # using getJobById from the job interface, sending a bad job id now
        # results in a SQLAlchemy exception rather than a 400. So for now, the
        # test is testing the test code. Arguably, we could remove this entirely
        # and replace it with a unit test as the logging and umbrella exeception
        # handling is refactored.
        jobId = -1
        with self.assertRaises(NoResultFound):
            self.run_test(jobId, 400, False, False, False, False, 0)

    def test_bad_prereq_job(self):
        """Test job with unfinished prerequisites."""
        jobId = self.jobDict["bad_prereq"]
        response = self.run_test(
            jobId, 400, "ready", False, False, "job_error", 0)

    def test_bad_type_job(self):
        """Test job with wrong type."""
        jobId = self.jobDict["wrong_type"]
        response = self.run_test(
            jobId, 400, "ready", False, False, "job_error", 0)


if __name__ == '__main__':
    unittest.main()