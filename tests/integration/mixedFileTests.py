from __future__ import print_function
from datetime import datetime
import unittest

from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import Job
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.models.stagingModels import AwardFinancial
from dataactvalidator.app import createApp
from tests.integration.baseTestValidator import BaseTestValidator
from tests.integration.fileTypeTests import FileTypeTests


class MixedFileTests(BaseTestValidator):

    RULES_TO_APPLY = ('A1', 'A16', 'A18', 'A19', 'A2', 'A20', 'A21', 'A24', 'A3', 'A4', 'B11', 'B12', 'B13', 'B3', 'B4',
                      'B5', 'B6', 'B7', 'B9/B10', 'C14', 'C17', 'C18', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9')

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources."""
        super(MixedFileTests, cls).setUpClass()
        user = cls.userId
        force_tas_load = False

        with createApp().app_context():
            # get the submission test user
            sess = GlobalDB.db().session

            # Create test submissions and jobs, also uploading
            # the files needed for each job.
            statusReadyId = JOB_STATUS_DICT['ready']
            jobTypeCsvId = JOB_TYPE_DICT['csv_record_validation']
            jobDict = {}

            # next three jobs belong to the same submission and are tests
            # for single-file validations that contain failing rows
            submissionId = cls.insertSubmission(sess, user)
            job_info = Job(
                filename=cls.uploadFile("appropMixed.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['mixed'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("programActivityMixed.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['program_activity'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['programMixed'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("awardMixed.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['award'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['awardMixed'] = job_info.job_id

            # next job tests single-file validations for award_financial
            # (submission has a non-Q1 end date)
            submissionId = cls.insertSubmission(sess, user, datetime(2015, 3, 15))
            job_info = Job(
                filename=cls.uploadFile("awardFinancialMixed.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['award_financial'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['awardFinMixed'] = job_info.job_id

            # job below tests a file that has a mixed-delimiter heading
            submissionId = cls.insertSubmission(sess, user)
            job_info = Job(
                filename=cls.uploadFile("awardMixedDelimiter.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['award'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['awardMixedDelimiter'] = job_info.job_id

            # next five jobs are cross-file and belong to the same submission
            submissionId = cls.insertSubmission(sess, user)
            job_info = Job(
                filename=cls.uploadFile("cross_file_A.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['crossApprop'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("cross_file_B.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['program_activity'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['crossPgmAct'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("cross_file_C.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['award_financial'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['crossAwardFin'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("cross_file_D2.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['award'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['crossAward'] = job_info.job_id

            job_info = Job(
                job_status_id=statusReadyId,
                job_type_id=JOB_TYPE_DICT['validation'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['crossFile'] = job_info.job_id

            # next four jobs test short columns names and belong to the same submission
            submissionId = cls.insertSubmission(sess, user)
            job_info = Job(
                filename=cls.uploadFile("appropValidShortcols.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['appropValidShortcols'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("programActivityMixedShortcols.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['program_activity'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['programMixedShortcols'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("awardFinancialMixedShortcols.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['award_financial'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['awardFinMixedShortcols'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("awardValidShortcols.csv", user),
                job_status_id=statusReadyId,
                job_type_id=jobTypeCsvId,
                file_type_id=FILE_TYPE_DICT['award'],
                submission_id=submissionId)
            sess.add(job_info)
            sess.flush()
            jobDict['awardValidShortcols'] = job_info.job_id

            # commit submissions/jobs and output IDs
            sess.commit()
            for job_type, job_id in jobDict.items():
                print('{}: {}'.format(job_type, job_id))

            # Load fields and rules
            FileTypeTests.load_definitions(cls.interfaces, force_tas_load, cls.RULES_TO_APPLY)

            cls.jobDict = jobDict

    def test_approp_valid_shortcol(self):
        """Test valid approp job with short colnames."""
        jobId = self.jobDict["appropValidShortcols"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0)

    def test_approp_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobDict["mixed"]
        self.passed = self.run_test(
            jobId, 200, "finished", 8212, 4, "complete", 39, 8, 869)

    def test_program_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobDict["programMixed"]
        self.passed = self.run_test(
        jobId, 200, "finished", 11390, 4, "complete", 81, 29, 9572)

    def test_program_mixed_shortcols(self):
        """Test object class/program activity job with some rows failing & short colnames."""
        jobId = self.jobDict["programMixedShortcols"]
        self.passed = self.run_test(
            jobId, 200, "finished", 11390, 4, "complete", 81, 29, 9572)

    def test_award_fin_mixed(self):
        """Test mixed award job with some rows failing."""
        jobId = self.jobDict["awardFinMixed"]
        self.passed = self.run_test(
        jobId, 200, "finished", 7537, 6, "complete", 47, 30, 8378)

        # Test that whitespace is converted to null
        rowThree = self.interfaces.validationDb.session.query(AwardFinancial).filter(AwardFinancial.parent_award_id == "ZZZZ").filter(AwardFinancial.submission_id == self.interfaces.jobDb.getSubmissionId(jobId)).first()
        self.assertIsNone(rowThree.agency_identifier)
        self.assertIsNone(rowThree.piid)
        # And commas removed for numeric
        rowThirteen = self.interfaces.validationDb.session.query(AwardFinancial).filter(AwardFinancial.parent_award_id == "YYYY").filter(AwardFinancial.submission_id == self.interfaces.jobDb.getSubmissionId(jobId)).first()
        self.assertEqual(rowThirteen.deobligations_recov_by_awa_cpe,26000)

    def test_award_fin_mixed_shortcols(self):
        """Test award financial job with some rows failing & short colnames."""
        jobId = self.jobDict["awardFinMixedShortcols"]
        self.passed = self.run_test(
            jobId, 200, "finished", 7537, 6, "complete", 47, 30, 8378)

    def test_award_valid_shortcols(self):
        """Test valid award (financial assistance) job with short colnames."""
        jobId = self.jobDict["awardValidShortcols"]
        self.passed = self.run_test(
            jobId, 200, "finished", 63, 10, "complete", 0)

    def test_award_mixed(self):
        """Test mixed job with some rows failing."""
        jobId = self.jobDict["awardMixed"]
        self.passed = self.run_test(
            jobId, 200, "finished", 123, 10, "complete", 1, 0, 63)

    def test_award_mixed_delimiter(self):
        """Test mixed job with mixed delimiter"""
        jobId = self.jobDict["awardMixedDelimiter"]
        self.passed = self.run_test(
            jobId, 400, "invalid", False, False, "header_error", 0)

    def test_cross_file(self):
        crossId = self.jobDict["crossFile"]
        # Run jobs for A, B, C, and D2, then cross file validation job
        # Note: test files used for cross validation use the short column names
        # as a way to ensure those are handled correctly by the validator
        awardFinResponse = self.validateJob(self.jobDict["crossAwardFin"])
        self.assertEqual(awardFinResponse.status_code, 200, msg=str(awardFinResponse.json))
        awardResponse = self.validateJob(self.jobDict["crossAward"])
        self.assertEqual(awardResponse.status_code, 200, msg=str(awardResponse.json))
        appropResponse = self.validateJob(self.jobDict["crossApprop"])
        self.assertEqual(appropResponse.status_code, 200, msg=str(appropResponse.json))
        pgmActResponse = self.validateJob(self.jobDict["crossPgmAct"])
        self.assertEqual(pgmActResponse.status_code, 200, msg=str(pgmActResponse.json))
        crossFileResponse = self.validateJob(crossId)
        self.assertEqual(crossFileResponse.status_code, 200, msg=str(crossFileResponse.json))

        # Check number of cross file validation errors in DB for this job
        self.assertEqual(self.interfaces.errorDb.checkNumberOfErrorsByJobId(crossId, self.interfaces.validationDb, "fatal"), 0)
        self.assertEqual(self.interfaces.errorDb.checkNumberOfErrorsByJobId(crossId, self.interfaces.validationDb, "warning"), 5)
        self.assertEqual(self.interfaces.jobDb.getJobStatus(crossId), JOB_STATUS_DICT['finished'])

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