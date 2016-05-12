from __future__ import print_function
from dataactcore.models.jobModels import JobDependency
from dataactcore.models.validationModels import Rule
from baseTestValidator import BaseTestValidator
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

        # Clear validation rules
        for fileType in ["award", "award_financial",
                "appropriations", "program_activity"]:
            validationDb.removeRulesByFileType(fileType)
            validationDb.removeColumnsByFileType(fileType)

        # Create submissions and get IDs back
        submissionIDs = {}
        for i in range(1, 17):
            submissionIDs[i] = cls.insertSubmission(
                jobTracker, userId=cls.userId)

        csvFiles = {
            "valid": {"filename": "testValid.csv", "status": "ready", "jobType": "csv_record_validation", "submissionLocalId": 1, "fileType": 1},
            "bad_upload": {"filename": "", "status": "ready", "jobType": "file_upload", "submissionLocalId": 2, "fileType": 1},
            "bad_prereq": {"filename": "", "status": "ready", "jobType": "csv_record_validation", "submissionLocalId" :2,  "fileType": 1},
            "wrong_type": {"filename": "", "status": "ready", "jobType": "external_validation", "submissionLocalId": 4, "fileType": 1},
            "not_ready": {"filename": "", "status": "finished", "jobType": "csv_record_validation", "submissionLocalId": 5, "fileType": 1},
            "valid_upload": {"filename": "", "status": "finished", "jobType": "file_upload", "submissionLocalId": 6, "fileType": 1},
            "valid_prereq": {"filename": "testPrereq.csv", "status": "ready", "jobType": "csv_record_validation", "submissionLocalId": 6, "fileType": 1},
            "bad_values": {"filename": "testBadValues.csv", "status": "ready", "jobType": "csv_record_validation", "submissionLocalId": 8, "fileType": 1},
            "mixed": {"filename": "testMixed.csv", "status": "ready", "jobType": "csv_record_validation", "submissionLocalId": 9, "fileType": 1},
            "empty": {"filename": "testEmpty.csv", "status": "ready", "jobType": "csv_record_validation", "submissionLocalId": 10, "fileType": 1},
            "missing_header": {"filename": "testMissingHeader.csv", "status": "ready", "jobType": "csv_record_validation", "submissionLocalId": 11, "fileType": 1},
            "bad_header": {"filename": "testBadHeader.csv", "status": "ready", "jobType": "csv_record_validation", "submissionLocalId": 12, "fileType": 2},
            "many": {"filename": "testMany.csv", "status": "ready", "jobType": "csv_record_validation", "submissionLocalId": 11, "fileType": 3},
            "odd_characters": {"filename": "testOddCharacters.csv", "status": "ready", "jobType": "csv_record_validation", "submissionLocalId":14, "fileType": 2},
            "many_bad": {"filename": "testManyBadValues.csv", "status": "ready", "jobType": "csv_record_validation", "submissionLocalId": 11, "fileType": 4},
            "rules": {"filename": "testRules.csv", "status":"ready", "jobType": "csv_record_validation", "submissionLocalId": 16, "fileType": 3}
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
                prerequisite_id = str(jobIdDict["bad_upload"])),
            JobDependency(
                job_id = str(jobIdDict["valid_prereq"]),
                prerequisite_id = str(jobIdDict["valid_upload"]))
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

        rules = [
            Rule(file_column_id = str(colIdDict["".join(["header_",str(1),"_file_type_",str(3)])]),rule_type_id = 5, rule_text_1 = 0, description =  'value 1 must be greater than zero', rule_timing_id=1),
            Rule(file_column_id = str(colIdDict["".join(["header_",str(1),"_file_type_",str(3)])]),rule_type_id = 3, rule_text_1 = 13, description =  'value 1 may not be 13', rule_timing_id=1),
            Rule(file_column_id = str(colIdDict["".join(["header_",str(5),"_file_type_",str(3)])]),rule_type_id = 1, rule_text_1 = "INT", description =  'value 5 must be an integer', rule_timing_id=1),
            Rule(file_column_id = str(colIdDict["".join(["header_",str(3),"_file_type_",str(3)])]),rule_type_id = 2, rule_text_1 = 42, description =  'value 3 must be equal to 42 if present', rule_timing_id=1),
            Rule(file_column_id = str(colIdDict["".join(["header_",str(1),"_file_type_",str(3)])]),rule_type_id = 4, rule_text_1 = 100, description =  'value 1 must be less than 100', rule_timing_id=1),
            Rule(file_column_id = str(colIdDict["".join(["header_",str(1),"_file_type_",str(3)])]),rule_type_id = 2, rule_text_1 = "  ", description =  'None shall pass', rule_timing_id=2) #This rule should never be checked with rule_timing 2
        ]

        for rule in rules:
            validationDb.session.add(rule)
        validationDb.session.commit()

        # If staging already has corresponding job tables, drop them
        for k, v in jobIdDict.items():
            try:
                cls.stagingDb.dropTable("job{}".format(v))
            except Exception as e:
                cls.stagingDb.session.close()
                cls.stagingDb.session = cls.stagingDb.Session()

        cls.jobIdDict = jobIdDict

    def test_valid_job(self):
        """Test valid job."""
        jobId = self.jobIdDict["valid"]
        response = self.run_test(
            jobId, 200, "finished", 52, 1, "complete", 0, False)

    def test_rules(self):
        """Test rules, should have one type failure and four value failures."""
        jobId = self.jobIdDict["rules"]
        response = self.run_test(
            jobId, 200, "finished", 350, 1, "complete", 5, True)

    def test_bad_values_job(self):
        """Test a job with bad values."""
        jobId = self.jobIdDict["bad_values"]
        response = self.run_test(
            jobId, 200, "finished", 5894, 0, "complete", 90, True)

    def test_many_bad_values_job(self):
        # Test job with many bad values
        if self.includeLongTests:
            jobId = self.jobIdDict["many_bad"]
            response = self.run_test(
                jobId, 200, "finished", 151665643, 0, "complete", 2302930, True)
        else:
            self.skipTest("includeLongTests flag is off")

    def test_mixed_job(self):
        """Test mixed job."""
        jobId = self.jobIdDict["mixed"]
        response = self.run_test(
            jobId, 200, "finished", 99, 3, "complete", 1, True)

    def test_empty(self):
        """Test empty file."""
        jobId = self.jobIdDict["empty"]
        if self.useThreads:
            status = 200
        else:
            status = 400
        response = self.run_test(
            jobId, status, "invalid", False, False, "single_row_error", 0, False)

        if not self.useThreads:
            self.assertEqual(
                response.json["message"], "CSV file must have a header")

    def test_missing_header(self):
        """Test missing header in first row."""
        jobId = self.jobIdDict["missing_header"]
        if self.useThreads:
            status = 200
        else:
            status = 400

        response = self.run_test(
            jobId, status, "invalid", False, False, "header_error", 0, False)

        if not self.useThreads:
            self.assertIn("Errors in header row", response.json["message"])

    def test_bad_header(self):
        """ Ignore bad header value in first row, then fail on a duplicate header """
        jobId = self.jobIdDict["bad_header"]
        if self.useThreads:
            status = 200
        else:
            status = 400

        response = self.run_test(
            jobId, status, "invalid", False, False, "header_error", 0, False)

        if not self.useThreads:
            self.assertIn("Errors in header row", response.json["message"])

    def test_many_rows(self):
        """Test many rows."""
        if self.includeLongTests:
            jobId = self.jobIdDict["many"]
            response = self.run_test(
                jobId, 200, "finished", 52, 22380, "complete", 0, False)
        else:
            self.skipTest("includeLongTests flag is off")

    def test_odd_characters(self):
        """Test potentially problematic characters."""
        jobId = self.jobIdDict["odd_characters"]
        response = self.run_test(
            jobId, 200, "finished", 99, 6, "complete", 1, True)

    def test_bad_id_job(self):
        """Test job ID not found in job table."""
        jobId = -1
        response = self.run_test(
            jobId, 400, False, False, False, False, 0, None)

    def test_prereq_job(self):
        """Test job with prerequisites finished."""
        jobId = self.jobIdDict["valid_prereq"]
        response = self.run_test(
            jobId, 200, "finished", 52, 4, "complete", 0, False)

    def test_bad_prereq_job(self):
        """Test job with unfinished prerequisites."""
        jobId = self.jobIdDict["bad_prereq"]
        response = self.run_test(
            jobId, 400, "ready", False, False, "job_error", 0, None)

    def test_bad_type_job(self):
        """Test job with wrong type."""
        jobId = self.jobIdDict["wrong_type"]
        response = self.run_test(
            jobId, 400, "ready", False, False, "job_error", 0, None)

    # TODO uncomment this unit test once jobs are labeled as ready
    # def test_finished_job(self):
    #     """ Test job that is already finished """
    #     jobId = self.jobIdDict["finished"]
    #     self.run_test(jobId,400,"finished",False,False,"job_error",0)

    def tearDown(self):
        super(JobTests, self).tearDown()
        # TODO: drop tables, etc.

if __name__ == '__main__':
    unittest.main()