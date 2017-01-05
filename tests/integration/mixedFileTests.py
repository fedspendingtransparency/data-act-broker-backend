from datetime import datetime

from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import checkNumberOfErrorsByJobId
from dataactcore.models.jobModels import Job
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.models.stagingModels import AwardFinancial
from dataactcore.utils.report import report_file_name
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
            status_ready_id = JOB_STATUS_DICT['ready']
            job_type_csv_id = JOB_TYPE_DICT['csv_record_validation']
            job_dict = {}

            # next three jobs belong to the same submission and are tests
            # for single-file validations that contain failing rows
            submission_id = cls.insertSubmission(sess, user)
            job_info = Job(
                filename=cls.uploadFile("appropMixed.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['mixed'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("programActivityMixed.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['program_activity'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['programMixed'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("awardMixed.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['award'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['awardMixed'] = job_info.job_id

            # next job tests single-file validations for award_financial
            # (submission has a non-Q1 end date)
            submission_id = cls.insertSubmission(sess, user, datetime(2015, 3, 15))
            job_info = Job(
                filename=cls.uploadFile("awardFinancialMixed.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['award_financial'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['awardFinMixed'] = job_info.job_id

            # job below tests a file that has a mixed-delimiter heading
            submission_id = cls.insertSubmission(sess, user)
            job_info = Job(
                filename=cls.uploadFile("awardMixedDelimiter.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['award'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['awardMixedDelimiter'] = job_info.job_id

            # next five jobs are cross-file and belong to the same submission
            submission_id = cls.insertSubmission(sess, user)
            job_info = Job(
                filename=cls.uploadFile("cross_file_A.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['crossApprop'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("cross_file_B.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['program_activity'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['crossPgmAct'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("cross_file_C.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['award_financial'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['crossAwardFin'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("cross_file_D2.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['award'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['crossAward'] = job_info.job_id

            job_info = Job(
                job_status_id=status_ready_id,
                job_type_id=JOB_TYPE_DICT['validation'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['crossFile'] = job_info.job_id

            # next four jobs test short columns names and belong to the same submission
            submission_id = cls.insertSubmission(sess, user)
            job_info = Job(
                filename=cls.uploadFile("appropValidShortcols.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['appropriations'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['appropValidShortcols'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("programActivityMixedShortcols.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['program_activity'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['programMixedShortcols'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("awardFinancialMixedShortcols.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['award_financial'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['awardFinMixedShortcols'] = job_info.job_id

            job_info = Job(
                filename=cls.uploadFile("awardValidShortcols.csv", user),
                job_status_id=status_ready_id,
                job_type_id=job_type_csv_id,
                file_type_id=FILE_TYPE_DICT['award'],
                submission_id=submission_id)
            sess.add(job_info)
            sess.flush()
            job_dict['awardValidShortcols'] = job_info.job_id

            # commit submissions/jobs and output IDs
            sess.commit()
            for job_type, job_id in job_dict.items():
                print('{}: {}'.format(job_type, job_id))

            # Load fields and rules
            FileTypeTests.load_definitions(sess, force_tas_load, cls.RULES_TO_APPLY)

            cls.job_dict = job_dict

    def test_approp_valid_shortcol(self):
        """Test valid approp job with short colnames."""
        job_id = self.job_dict["appropValidShortcols"]
        self.passed = self.run_test(
            job_id, 200, "finished", 63, 10, "complete", 0)

    def test_approp_mixed(self):
        """Test mixed job with some rows failing."""
        job_id = self.job_dict["mixed"]
        self.passed = self.run_test(
            job_id, 200, "finished", 6681, 5, "complete", 27, 7, 656)

    def test_program_mixed(self):
        """Test mixed job with some rows failing."""
        job_id = self.job_dict["programMixed"]
        self.passed = self.run_test(
            job_id, 200, "finished", 8268, 5, "complete", 46, 24, 7697)

    def test_program_mixed_shortcols(self):
        """Test object class/program activity job with some rows failing & short colnames."""
        job_id = self.job_dict["programMixedShortcols"]
        self.passed = self.run_test(
            job_id, 200, "finished", 8268, 5, "complete", 46, 24, 7697)

    def test_award_fin_mixed(self):
        """Test mixed award job with some rows failing."""
        job_id = self.job_dict["awardFinMixed"]
        self.passed = self.run_test(
            job_id, 200, "finished", 8661, 5, "complete", 51, 32, 7968)

        with createApp().app_context():
            sess = GlobalDB.db().session
            job = sess.query(Job).filter_by(job_id=job_id).one()
            # todo: these whitespace and comma cases probably belong in unit tests
            # Test that whitespace is converted to null
            row_three = sess.query(AwardFinancial).\
                filter(AwardFinancial.parent_award_id == "ZZZZ", AwardFinancial.submission_id == job.submission_id).\
                first()
            self.assertIsNone(row_three.agency_identifier)
            self.assertIsNone(row_three.piid)
            # Test that commas are removed for numeric values
            row_thirteen = sess.query(AwardFinancial).\
                filter(AwardFinancial.parent_award_id == "YYYY", AwardFinancial.submission_id == job.submission_id).\
                first()
            self.assertEqual(row_thirteen.deobligations_recov_by_awa_cpe, 26000)

    def test_award_fin_mixed_shortcols(self):
        """Test award financial job with some rows failing & short colnames."""
        job_id = self.job_dict["awardFinMixedShortcols"]
        self.passed = self.run_test(
            job_id, 200, "finished", 8661, 5, "complete", 51, 32, 7968)

    def test_award_valid_shortcols(self):
        """Test valid award (financial assistance) job with short colnames."""
        job_id = self.job_dict["awardValidShortcols"]
        self.passed = self.run_test(
            job_id, 200, "finished", 63, 10, "complete", 0)

    def test_award_mixed(self):
        """Test mixed job with some rows failing."""
        job_id = self.job_dict["awardMixed"]
        self.passed = self.run_test(
            job_id, 200, "finished", 123, 10, "complete", 1, 0, 63)

    def test_award_mixed_delimiter(self):
        """Test mixed job with mixed delimiter"""
        job_id = self.job_dict["awardMixedDelimiter"]
        self.passed = self.run_test(
            job_id, 400, "invalid", False, False, "header_error", 0)

    def test_cross_file(self):
        cross_id = self.job_dict["crossFile"]
        # Run jobs for A, B, C, and D2, then cross file validation job
        # Note: test files used for cross validation use the short column names
        # as a way to ensure those are handled correctly by the validator
        award_fin_resp = self.validateJob(self.job_dict["crossAwardFin"])
        self.assertEqual(award_fin_resp.status_code, 200, msg=str(award_fin_resp.json))
        award_resp = self.validateJob(self.job_dict["crossAward"])
        self.assertEqual(award_resp.status_code, 200, msg=str(award_resp.json))
        approp_resp = self.validateJob(self.job_dict["crossApprop"])
        self.assertEqual(approp_resp.status_code, 200, msg=str(approp_resp.json))
        pgm_act_resp = self.validateJob(self.job_dict["crossPgmAct"])
        self.assertEqual(pgm_act_resp.status_code, 200, msg=str(pgm_act_resp.json))
        cross_file_resp = self.validateJob(cross_id)
        self.assertEqual(cross_file_resp.status_code, 200, msg=str(cross_file_resp.json))

        with createApp().app_context():
            sess = GlobalDB.db().session

            job = sess.query(Job).filter(Job.job_id == cross_id).one()

            # Check number of cross file validation errors in DB for this job
            self.assertEqual(checkNumberOfErrorsByJobId(cross_id, "fatal"), 0)
            self.assertEqual(checkNumberOfErrorsByJobId(cross_id, "warning"), 3)
            self.assertEqual(job.job_status_id, JOB_STATUS_DICT['finished'])

            # Check that cross file validation report exists and is the right size
            submission_id = job.submission_id
            size_path_pairs = [
                (89, report_file_name(submission_id, False, "appropriations",
                                      "program_activity")),
                (89, report_file_name(submission_id, False, "award_financial",
                                      "award")),
                (2363, report_file_name(submission_id, True, "appropriations",
                                        "program_activity")),
                (89, report_file_name(submission_id, True, "award_financial",
                                      "award")),
            ]

        for size, path in size_path_pairs:
            if self.local:
                self.assertFileSizeAppxy(size, path)
            else:
                self.assertGreater(
                    s3UrlHandler.getFileSize("errors/" + path), size - 5)
                self.assertLess(
                    s3UrlHandler.getFileSize("errors/" + path), size + 5)
