from __future__ import print_function
from dataactcore.interfaces.db import databaseSession
from dataactcore.models.jobModels import JobDependency
from dataactvalidator.filestreaming.schemaLoader import SchemaLoader
from tests.integration.baseTestValidator import BaseTestValidator
import unittest

class JobTests(BaseTestValidator):

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources (test data)"""
        super(JobTests, cls).setUpClass()
        #TODO: refactor into a pytest fixture

        # Flag for testing a million+ errors (can take ~30 min to run)
        cls.includeLongTests = False

        validationDb = cls.validationDb
        jobTracker = cls.jobTracker

        # Create submissions and get IDs back
        submissionIDs = {}
        for i in range(1, 17):
            submissionIDs[i] = cls.insertSubmission(
                jobTracker, userId=cls.userId)

        csvFiles = {
            "bad_upload": {"filename": "", "status": "ready", "jobType": "file_upload", "submissionLocalId": 2, "fileType": 1},
            "bad_prereq": {"filename": "", "status": "ready", "jobType": "csv_record_validation", "submissionLocalId" :2,  "fileType": 1},
            "wrong_type": {"filename": "", "status": "ready", "jobType": "external_validation", "submissionLocalId": 4, "fileType": 1},
            "not_ready": {"filename": "", "status": "finished", "jobType": "csv_record_validation", "submissionLocalId": 5, "fileType": 1},
            "empty": {"filename": "testEmpty.csv", "status": "ready", "jobType": "csv_record_validation", "submissionLocalId": 10, "fileType": 1},
        }

        # Upload needed files to S3
        for key in csvFiles.keys():
            csvFiles[key]["s3Filename"] = cls.uploadFile(
                csvFiles[key]["filename"], cls.userId)
        jobIdDict = {}

        for key in csvFiles.keys():
            file = csvFiles[key]
            job = cls.addJob(
                str(jobTracker.getJobStatusId(file["status"])),
                str(jobTracker.getJobTypeId(file["jobType"])),
                str(submissionIDs[file["submissionLocalId"]]),
                file["s3Filename"],
                str(file["fileType"]),
                jobTracker.session)
            # TODO: fix statement below--does this error really happen?
            if(job.job_id == None):
                # Failed to commit job correctly
                raise Exception(
                    "".join(["Job for ", str(key), " did not get an id back"]))
            jobIdDict[key] = job.job_id
            # Print submission IDs for error report checking
            print("".join([str(key),": ",str(jobTracker.getSubmissionId(job.job_id)), ", "]), end = "")

        # Create dependencies
        dependencies = [
            JobDependency(
                job_id = str(jobIdDict["bad_prereq"]),
                prerequisite_id = str(jobIdDict["bad_upload"]))
        ]

        for dependency in dependencies:
            jobTracker.session.add(dependency)
        jobTracker.session.commit()

        colIdDict = {}
        for fileId in range(1, 5):
            for columnId in range(1, 6):
                #TODO: get rid of hard-coded surrogate keys
                if columnId < 3:
                    fieldType = 1
                else:
                    fieldType = 4
                columnName = "header_{}".format(columnId)
                column = cls.addFileColumn(
                    fileId, fieldType, columnName, "",
                    (columnId != 3), validationDb.session)
                colIdDict["header_{}_file_type_{}".format(
                    columnId, fileId)] = column.file_column_id

        cls.jobIdDict = jobIdDict

    def tearDown(self):
        super(JobTests, self).tearDown()
        # TODO: drop tables, etc.

    def test_empty(self):
        """Test empty file."""
        jobId = self.jobIdDict["empty"]
        if self.useThreads:
            status = 200
        else:
            status = 400
        response = self.run_test(
            jobId, status, "invalid", False, False, "single_row_error", 0)

        if not self.useThreads:
            self.assertEqual(
                response.json["message"], "CSV file must have a header")

    def test_bad_id_job(self):
        """Test job ID not found in job table."""
        jobId = -1
        response = self.run_test(
            jobId, 400, False, False, False, False, 0)

    def test_bad_prereq_job(self):
        """Test job with unfinished prerequisites."""
        jobId = self.jobIdDict["bad_prereq"]
        response = self.run_test(
            jobId, 400, "ready", False, False, "job_error", 0)

    def test_bad_type_job(self):
        """Test job with wrong type."""
        jobId = self.jobIdDict["wrong_type"]
        response = self.run_test(
            jobId, 400, "ready", False, False, "job_error", 0)

    # removing long tests because 1) we don't run them and 2) the file formats need updated
    # TODO: add a compliant file for this test if we want to use it
    # def test_many_bad_values_job(self):
    #     # Test job with many bad values
    #     if self.includeLongTests:
    #         jobId = self.jobIdDict["many_bad"]
    #         response = self.run_test(
    #             jobId, 200, "finished", 151665643, 0, "complete", 2302930)
    #     else:
    #         self.skipTest("includeLongTests flag is off")

    # removing long tests because 1) we don't run them and 2) the file formats need updated
    # TODO: add a compliant file for this test if we want to use it
    # def test_many_rows(self):
    #     """Test many rows."""
    #     if self.includeLongTests:
    #         jobId = self.jobIdDict["many"]
    #         response = self.run_test(
    #             jobId, 200, "finished", 52, 22380, "complete", 0)
    #     else:
    #         self.skipTest("includeLongTests flag is off")

    # TODO uncomment this test if we start limiting the validator to only jobs that are "ready"
    #def test_finished_job(self):
    #    """ Test job that is already finished """
    #    jobId = self.jobIdDict["finished"]
    #    self.run_test(jobId,400,"finished",False,False,"job_error",0)


if __name__ == '__main__':
    unittest.main()