import os
from collections import namedtuple
from csv import reader
from datetime import datetime
import logging
from dateutil.relativedelta import relativedelta
from uuid import uuid4
from shutil import copyfile

import requests
from flask import g, request
from requests.exceptions import Timeout
import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy.orm.exc import NoResultFound
from werkzeug.utils import secure_filename

from dataactbroker.permissions import current_user_can, current_user_can_on_submission
from dataactcore.aws.s3UrlHandler import S3UrlHandler
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import CGAC, SubTierAgency
from dataactcore.models.errorModels import File
from dataactcore.models.stagingModels import DetachedAwardFinancialAssistance, PublishedAwardFinancialAssistance
from dataactcore.models.jobModels import (
    FileGenerationTask, Job, Submission, SubmissionNarrative, JobDependency, SubmissionSubTierAffiliation,
    RevalidationThreshold)
from dataactcore.models.userModel import User
from dataactcore.models.lookups import (
    FILE_TYPE_DICT, FILE_TYPE_DICT_LETTER, FILE_TYPE_DICT_LETTER_ID, PUBLISH_STATUS_DICT,
    JOB_STATUS_DICT, JOB_TYPE_DICT, RULE_SEVERITY_DICT, FILE_TYPE_DICT_ID, JOB_STATUS_DICT_ID, FILE_STATUS_DICT)
from dataactcore.utils.jobQueue import generate_e_file, generate_f_file
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.report import get_cross_file_pairs, report_file_name
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.stringCleaner import StringCleaner
from dataactcore.interfaces.function_bag import (
    check_number_of_errors_by_job_id, create_jobs, create_submission, get_error_metrics_by_job_jd, get_error_type,
    get_submission_status, mark_job_status, run_job_checks, create_file_if_needed, get_last_validated_date)
from dataactvalidator.filestreaming.csv_selection import write_csv

logger = logging.getLogger(__name__)


class FileHandler:
    """ Responsible for all tasks relating to file upload

    Static fields:
    FILE_TYPES -- list of file labels that can be included

    Instance fields:
    request -- A flask request object, comes with the request
    s3manager -- instance of S3UrlHandler, manages calls to S3
    """

    FILE_TYPES = ["appropriations", "award_financial", "program_activity"]
    EXTERNAL_FILE_TYPES = ["award", "award_procurement", "awardee_attributes", "sub_award"]
    VALIDATOR_RESPONSE_FILE = "validatorResponse"
    STATUS_MAP = {"waiting": "invalid", "ready": "invalid", "running": "waiting", "finished": "finished",
                  "invalid": "failed", "failed": "failed"}
    VALIDATION_STATUS_MAP = {"waiting": "waiting", "ready": "waiting", "running": "waiting", "finished": "finished",
                             "failed": "failed", "invalid": "failed"}

    UploadFile = namedtuple('UploadFile', ['file_type', 'upload_name', 'file_name', 'file_letter'])

    def __init__(self, route_request, is_local=False, server_path=""):
        """ Create the File Handler

        Arguments:
            route_request - HTTP request object for this route
            isLocal - True if this is a local installation that will not use AWS or Smartronix
            serverPath - If isLocal is True, this is used as the path to local files
        """
        self.request = route_request
        self.isLocal = is_local
        self.serverPath = server_path
        self.s3manager = S3UrlHandler()

    def get_error_report_urls_for_submission(self, submission_id, is_warning=False):
        """
        Gets the Signed URLs for download based on the submissionId
        """
        sess = GlobalDB.db().session
        try:
            self.s3manager = S3UrlHandler()
            response_dict = {}
            jobs = sess.query(Job).filter_by(submission_id=submission_id)
            for job in jobs:
                if job.job_type.name == 'csv_record_validation':
                    report_name = report_file_name(
                        job.submission_id, is_warning, job.file_type.name)
                    if is_warning:
                        key = 'job_{}_warning_url'.format(job.job_id)
                    else:
                        key = 'job_{}_error_url'.format(job.job_id)
                    if not self.isLocal:
                        response_dict[key] = self.s3manager.get_signed_url("errors", report_name, method="GET")
                    else:
                        path = os.path.join(self.serverPath, report_name)
                        response_dict[key] = path

            # For each pair of files, get url for the report
            for c in get_cross_file_pairs():
                first_file = c[0]
                second_file = c[1]
                report_name = report_file_name(
                    submission_id, is_warning, first_file.name,
                    second_file.name
                )
                if self.isLocal:
                    report_path = os.path.join(self.serverPath, report_name)
                else:
                    report_path = self.s3manager.get_signed_url("errors", report_name, method="GET")
                # Assign to key based on source and target
                response_dict[get_cross_report_key(first_file.name, second_file.name, is_warning)] = report_path

            return JsonResponse.create(StatusCode.OK, response_dict)

        except ResponseException as e:
            return JsonResponse.error(e, StatusCode.CLIENT_ERROR)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)

    def submit(self, create_credentials):
        """ Builds S3 URLs for a set of files and adds all related jobs to job tracker database

        Flask request should include keys from FILE_TYPES class variable above

        Arguments:
            create_credentials - If True, will create temporary credentials for S3 uploads

        Returns:
        Flask response returned will have key_url and key_id for each key in the request
        key_url is the S3 URL for uploading
        key_id is the job id to be passed to the finalize_submission route
        """
        sess = GlobalDB.db().session
        try:
            response_dict = {}
            upload_files = []
            request_params = RequestDictionary.derive(self.request)

            # unfortunately, field names in the request don't match
            # field names in the db/response. create a mapping here.
            request_submission_mapping = {
                "cgac_code": "cgac_code",
                "reporting_period_start_date": "reporting_start_date",
                "reporting_period_end_date": "reporting_end_date",
                "is_quarter": "is_quarter_format"}

            submission_data = {}
            existing_submission_id = request_params.get('existing_submission_id')
            if existing_submission_id:
                existing_submission = True
                existing_submission_obj = sess.query(Submission).\
                    filter_by(submission_id=existing_submission_id).\
                    one()
            else:
                existing_submission = None
                existing_submission_obj = None
            for request_field, submission_field in request_submission_mapping.items():
                if request_field in request_params:
                    request_value = request_params[request_field]
                    submission_data[submission_field] = request_value
                # all of those fields are required unless
                # existing_submission_id is present
                elif 'existing_submission_id' not in request_params:
                    raise ResponseException('{} is required'.format(request_field), StatusCode.CLIENT_ERROR, ValueError)
            # make sure submission dates are valid
            formatted_start_date, formatted_end_date = FileHandler.check_submission_dates(
                submission_data.get('reporting_start_date'),
                submission_data.get('reporting_end_date'),
                submission_data.get('is_quarter_format'),
                existing_submission_obj)
            submission_data['reporting_start_date'] = formatted_start_date
            submission_data['reporting_end_date'] = formatted_end_date

            submission = create_submission(g.user.user_id, submission_data,
                                           existing_submission_obj)
            cant_edit = (
                existing_submission and
                not current_user_can_on_submission(
                    'writer', existing_submission_obj)
            )
            cant_create = not current_user_can('writer', submission.cgac_code)
            if cant_edit or cant_create:
                raise ResponseException(
                    "User does not have permission to create/modify that "
                    "submission", StatusCode.PERMISSION_DENIED
                )
            else:
                sess.add(submission)
                sess.commit()

            # build fileNameMap to be used in creating jobs
            self.build_file_map(request_params, FileHandler.FILE_TYPES, response_dict, upload_files,
                                existing_submission)

            if not upload_files and existing_submission:
                raise ResponseException("Must include at least one file for an existing submission",
                                        StatusCode.CLIENT_ERROR)
            if not existing_submission:
                # don't add external files to existing submission
                for ext_file_type in FileHandler.EXTERNAL_FILE_TYPES:
                    filename = CONFIG_BROKER["".join([ext_file_type, "_file_name"])]

                    if not self.isLocal:
                        upload_name = "{}/{}".format(
                            g.user.user_id,
                            S3UrlHandler.get_timestamped_filename(filename)
                        )
                    else:
                        upload_name = filename
                    response_dict[ext_file_type + "_key"] = upload_name
                    upload_files.append(FileHandler.UploadFile(
                        file_type=ext_file_type,
                        upload_name=upload_name,
                        file_name=filename,
                        file_letter=FILE_TYPE_DICT_LETTER[FILE_TYPE_DICT[ext_file_type]]
                    ))

            self.create_response_dict_for_submission(upload_files, submission, existing_submission, response_dict,
                                                     create_credentials)
            return JsonResponse.create(StatusCode.OK, response_dict)
        except (ValueError, TypeError, NotImplementedError) as e:
            return JsonResponse.error(e, StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            # call error route directly, status code depends on exception
            return JsonResponse.error(e, e.status)
        except Exception as e:
            # unexpected exception, this is a 500 server error
            return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)

    @staticmethod
    def check_submission_dates(start_date, end_date, is_quarter, existing_submission=None):
        """Check validity of incoming submission start and end dates."""
        # if any of the date fields are none, there should be an existing submission
        # otherwise, we shouldn't be here
        if None in (start_date, end_date, is_quarter) and existing_submission is None:
            raise ResponseException("An existing submission is required when start/end date"
                                    " or is_quarter aren't supplied", StatusCode.INTERNAL_ERROR)

        # convert submission start/end dates from the request into Python date
        # objects. if a date is missing, grab it from the existing submission
        # note: a previous check ensures that there's an existing submission
        # when the start/end dates are empty
        date_format = '%m/%Y'
        try:
            if start_date is not None:
                start_date = datetime.strptime(start_date, date_format).date()
            else:
                start_date = existing_submission.reporting_start_date
            if end_date is not None:
                end_date = datetime.strptime(end_date, date_format).date()
            else:
                end_date = existing_submission.reporting_end_date
        except ValueError:
            raise ResponseException("Date must be provided as MM/YYYY", StatusCode.CLIENT_ERROR,
                                    ValueError)

        # the front-end is doing date checks, but we'll also do a few server side to ensure
        # everything is correct when clients call the API directly
        if start_date > end_date:
            raise ResponseException(
                "Submission start date {} is after the end date {}".format(start_date, end_date),
                StatusCode.CLIENT_ERROR)

        # currently, broker allows quarterly submissions for a single quarter only. the front-end
        # handles this requirement, but since we have some downstream logic that depends on a
        # quarterly submission representing one quarter, we'll check server side as well
        is_quarter = is_quarter if is_quarter is not None else existing_submission.is_quarter_format
        if is_quarter is None:
            is_quarter = existing_submission.is_quarter_format
        if is_quarter:
            if relativedelta(end_date + relativedelta(months=1), start_date).months != 3:
                raise ResponseException(
                    "Quarterly submission must span 3 months", StatusCode.CLIENT_ERROR)
            if end_date.month % 3 != 0:
                raise ResponseException(
                    "Invalid end month for a quarterly submission: {}".format(end_date.month),
                    StatusCode.CLIENT_ERROR)

        return start_date, end_date

    @staticmethod
    def finalize(job_id):
        """ Set upload job in job tracker database to finished, allowing dependent jobs to be started

        Flask request should include key "upload_id", which holds the job_id for the file_upload job

        Returns:
        A flask response object, if successful just contains key "success" with value True, otherwise value is False
        """
        sess = GlobalDB.db().session
        response_dict = {}
        try:
            # Compare user ID with user who submitted job, if no match return 400
            job = sess.query(Job).filter_by(job_id=job_id).one()
            submission = sess.query(Submission).filter_by(submission_id=job.submission_id).one()
            if not current_user_can_on_submission('writer', submission):
                # This user cannot finalize this job
                raise ResponseException(
                    "Cannot finalize a job for a different agency",
                    StatusCode.CLIENT_ERROR
                )
            # Change job status to finished
            if job.job_type_id == JOB_TYPE_DICT["file_upload"]:
                mark_job_status(job_id, 'finished')
                response_dict["success"] = True
                return JsonResponse.create(StatusCode.OK, response_dict)
            else:
                raise ResponseException("Wrong job type for finalize route", StatusCode.CLIENT_ERROR)

        except (ValueError, TypeError) as e:
            return JsonResponse.error(e, StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            return JsonResponse.error(e, e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)

    def upload_file(self):
        """ Saves a file and returns the saved path.  Should only be used for local installs. """
        try:
            if self.isLocal:
                uploaded_file = request.files['file']
                if uploaded_file:
                    seconds = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())
                    filename = "".join([str(seconds), "_", secure_filename(uploaded_file.filename)])
                    path = os.path.join(self.serverPath, filename)
                    uploaded_file.save(path)
                    return_dict = {"path": path}
                    return JsonResponse.create(StatusCode.OK, return_dict)
                else:
                    raise ResponseException("Failure to read file",
                                            StatusCode.CLIENT_ERROR)
            else:
                raise ResponseException("Route Only Valid For Local Installs",
                                        StatusCode.CLIENT_ERROR)
        except (ValueError, TypeError) as e:
            return JsonResponse.error(e, StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            return JsonResponse.error(e, e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)

    def start_generation_job(self, job):
        """ Initiates a file generation job

        Args:
            job: the file generation job to start

        Returns:
            Tuple of boolean indicating successful start, and error response if False

        """
        sess = GlobalDB.db().session
        file_type_name = job.file_type.name
        file_type = job.file_type.letter_name

        try:
            if file_type in ['D1', 'D2']:
                # Populate start and end dates, these should be provided in
                # MM/DD/YYYY format, using calendar year (not fiscal year)
                request_dict = RequestDictionary(self.request)
                start_date = request_dict.get_value("start")
                end_date = request_dict.get_value("end")

                if not (StringCleaner.is_date(start_date) and StringCleaner.is_date(end_date)):
                    raise ResponseException(
                        "Start or end date cannot be parsed into a date",
                        StatusCode.CLIENT_ERROR
                    )
            elif file_type not in ["E", "F"]:
                raise ResponseException(
                    "File type must be either D1, D2, E or F",
                    StatusCode.CLIENT_ERROR
                )
        except ResponseException as e:
            return False, JsonResponse.error(e, e.status, file_type=file_type, status='failed')

        submission = sess.query(Submission).filter_by(submission_id=job.submission_id).one()
        cgac_code = submission.cgac_code

        # Generate and upload file to S3
        job = self.add_generation_job_info(file_type_name=file_type_name, job=job)
        upload_file_name, timestamped_name = job.filename, job.original_filename

        if file_type in ["D1", "D2"]:
            logger.debug('Adding job info for job id of %s', job.job_id)
            return self.add_job_info_for_d_file(upload_file_name, timestamped_name, submission.submission_id, file_type,
                                                file_type_name, start_date, end_date, cgac_code, job)
        elif file_type == 'E':
            generate_e_file.delay(
                submission.submission_id, job.job_id, timestamped_name,
                upload_file_name, self.isLocal)
        elif file_type == 'F':
            generate_f_file.delay(
                submission.submission_id, job.job_id, timestamped_name,
                upload_file_name, self.isLocal)

        return True, None

    def add_job_info_for_d_file(self, upload_file_name, timestamped_name, submission_id, file_type, file_type_name,
                                start_date, end_date, cgac_code, job):
        """ Populates upload and validation job objects with start and end dates, filenames, and status

        Args:
            upload_file_name - Filename to use on S3
            timestamped_name - Version of filename without user ID
            submission_id - Submission to add D files to
            file_type - File type as either "D1" or "D2"
            file_type_name - Full name of file type
            start_date - Beginning of period for D file
            end_date - End of period for D file
            cgac_code - Agency to generate D file for
            job - Job object for upload job
        """
        sess = GlobalDB.db().session
        val_job = sess.query(Job).filter_by(
            submission_id=submission_id,
            file_type_id=FILE_TYPE_DICT[file_type_name],
            job_type_id=JOB_TYPE_DICT['csv_record_validation']
        ).one()
        try:
            val_job.filename = upload_file_name
            val_job.original_filename = timestamped_name
            val_job.job_status_id = JOB_STATUS_DICT["waiting"]
            job.start_date = datetime.strptime(start_date, "%m/%d/%Y").date()
            job.end_date = datetime.strptime(end_date, "%m/%d/%Y").date()
            val_job.start_date = datetime.strptime(start_date, "%m/%d/%Y").date()
            val_job.end_date = datetime.strptime(end_date, "%m/%d/%Y").date()
        except ValueError as e:
            # Date was not in expected format
            exc = ResponseException(str(e), StatusCode.CLIENT_ERROR, ValueError)
            return False, JsonResponse.error(
                exc, exc.status, url="", start="", end="",
                file_type=file_type
            )

        error = self.call_d_file_api(file_type_name, cgac_code, start_date, end_date, job, val_job)

        return not error, error

    def call_d_file_api(self, file_type_name, cgac_code, start_date, end_date, job, val_job=None):
        """ Call D file API, return True if results found, False otherwise """
        file_type = FILE_TYPE_DICT_LETTER[FILE_TYPE_DICT[file_type_name]]
        task_key = FileHandler.create_generation_task(job.job_id)

        if not self.isLocal:
            # Create file D API URL with dates and callback URL
            api_url = FileHandler.get_d_file_url(task_key, file_type_name, cgac_code, start_date, end_date)

            logger.debug('Calling D file API => %s', api_url)
            try:
                # Check for numFound = 0
                if "numFound='0'" in get_xml_response_content(api_url):
                    sess = GlobalDB.db().session
                    # No results found, skip validation and mark as finished
                    sess.query(JobDependency). \
                        filter(JobDependency.prerequisite_id == job.job_id). \
                        delete(synchronize_session='fetch')
                    mark_job_status(job.job_id, "finished")
                    job.filename = None

                    if val_job is not None:
                        mark_job_status(val_job.job_id, "finished")
                        # Create File object for this validation job
                        val_file = create_file_if_needed(val_job.job_id, filename=val_job.filename)
                        val_file.file_status_id = FILE_STATUS_DICT['complete']
                        val_job.number_of_rows = 0
                        val_job.number_of_rows_valid = 0
                        val_job.file_size = 0
                        val_job.number_of_errors = 0
                        val_job.number_of_warnings = 0
                        val_job.filename = None
                        # Update last validated date
                        val_job.last_validated = datetime.utcnow()
                    sess.commit()
            except Timeout as e:
                exc = ResponseException(str(e), StatusCode.CLIENT_ERROR, Timeout)
                return JsonResponse.error(e, exc.status, url="", start="", end="", file_type=file_type)
        else:
            self.complete_generation(task_key, file_type)

    def download_file(self, local_file_path, file_url):
        """ Download a file locally from the specified URL, returns True if successful """
        if not self.isLocal:
            with open(local_file_path, "w") as file:
                # get request
                response = requests.get(file_url)
                if response.status_code != 200:
                    # Could not download the file, return False
                    return False
                # write to file
                response.encoding = "utf-8"
                file.write(response.text)
                return True
        elif not os.path.isfile(file_url):
            raise ResponseException('{} does not exist'.format(file_url),
                                    StatusCode.INTERNAL_ERROR)
        elif not os.path.isdir(os.path.dirname(local_file_path)):
            dirname = os.path.dirname(local_file_path)
            raise ResponseException('{} folder does not exist'.format(dirname),
                                    StatusCode.INTERNAL_ERROR)
        else:
            copyfile(file_url, local_file_path)
            return True

    def load_d_file(self, url, upload_name, timestamped_name, job_id, is_local):
        """ Pull D file from specified URL and write to S3 """
        sess = GlobalDB.db().session
        try:
            full_file_path = "".join([CONFIG_BROKER['d_file_storage_path'], timestamped_name])

            logger.debug('Downloading file...')
            if not self.download_file(full_file_path, url):
                # Error occurred while downloading file, mark job as failed and record error message
                mark_job_status(job_id, "failed")
                job = sess.query(Job).filter_by(job_id=job_id).one()
                file_type = job.file_type.name
                if file_type == "award":
                    source = "ASP"
                elif file_type == "award_procurement":
                    source = "FPDS"
                else:
                    source = "unknown source"
                job.error_message = "A problem occurred receiving data from {}".format(source)

                raise ResponseException(job.error_message, StatusCode.CLIENT_ERROR)
            lines = get_lines_from_csv(full_file_path)

            write_csv(timestamped_name, upload_name, is_local, lines[0], lines[1:])

            logger.debug('Marking job id of %s', job_id)
            mark_job_status(job_id, "finished")
            return {"message": "Success", "file_name": timestamped_name}
        except Exception as e:
            logger.exception('Exception caught => %s', e)
            # Log the error
            JsonResponse.error(e, 500)
            sess.query(Job).filter_by(job_id=job_id).one().error_message = str(e)
            mark_job_status(job_id, "failed")
            sess.commit()
            raise e

    def generate_file(self, submission_id, file_type):
        """ Start a file generation job for the specified file type """
        logger.debug('Starting D file generation')
        logger.debug('Submission ID = %s / File type = %s',
                     submission_id, file_type)

        sess = GlobalDB.db().session

        # Check permission to submission
        error = submission_error(submission_id, file_type)
        if error:
            return error

        job = sess.query(Job).filter_by(
            submission_id=submission_id,
            file_type_id=FILE_TYPE_DICT_LETTER_ID[file_type],
            job_type_id=JOB_TYPE_DICT['file_upload']
        ).one()

        try:
            # Check prerequisites on upload job
            if not run_job_checks(job.job_id):
                raise ResponseException(
                    "Must wait for completion of prerequisite validation job",
                    StatusCode.CLIENT_ERROR
                )
        except ResponseException as exc:
            return JsonResponse.error(exc, exc.status)

        success, error_response = self.start_generation_job(job)

        logger.debug('Finished start_generation_job method')
        if not success:
            # If not successful, set job status as "failed"
            mark_job_status(job.job_id, "failed")
            return error_response

        # Return same response as check generation route
        submission = sess.query(Submission).\
            filter_by(submission_id=submission_id).\
            one()
        return self.check_generation(submission, file_type)

    def generate_detached_file(self, file_type, cgac_code, start, end):
        """ Start a file generation job for the specified file type """
        logger.debug("Starting detached D file generation")

        # check if date format is MM/DD/YYYY
        if not (StringCleaner.is_date(start) and StringCleaner.is_date(end)):
            raise ResponseException("Start or end date cannot be parsed into a date", StatusCode.CLIENT_ERROR)

        # add job info
        file_type_name = FILE_TYPE_DICT_ID[FILE_TYPE_DICT_LETTER_ID[file_type]]
        new_job = self.add_generation_job_info(
            file_type_name=file_type_name,
            dates={'start_date': start, 'end_date': end}
        )

        result = self.call_d_file_api(file_type_name, cgac_code, start, end, new_job)

        # Return same response as check generation route
        return result or self.check_detached_generation(new_job.job_id)

    def upload_detached_file(self, create_credentials):
        """ Builds S3 URLs for a set of detached files and adds all related jobs to job tracker database

        Flask request should include keys from FILE_TYPES class variable above

        Arguments:
            create_credentials - If True, will create temporary credentials for S3 uploads

        Returns:
        Flask response returned will have key_url and key_id for each key in the request
        key_url is the S3 URL for uploading
        key_id is the job id to be passed to the finalize_submission route
        """
        logger.debug("Starting detached D file upload")
        sess = GlobalDB.db().session
        try:
            response_dict = {}
            upload_files = []
            request_params = RequestDictionary.derive(self.request)

            # unfortunately, field names in the request don't match
            # field names in the db/response. create a mapping here.
            request_job_mapping = {
                "reporting_period_start_date": "reporting_start_date",
                "reporting_period_end_date": "reporting_end_date"
            }

            job_data = {}
            for request_field, submission_field in request_job_mapping.items():
                if request_field in request_params:
                    job_data[submission_field] = request_params[request_field]
                # all of those fields are required
                else:
                    raise ResponseException('{} is required'.format(request_field), StatusCode.CLIENT_ERROR, ValueError)

            # get the cgac code associated with this sub tier agency
            sub_tier_agency = sess.query(SubTierAgency).\
                filter_by(sub_tier_agency_code=request_params["agency_code"]).one()
            job_data["cgac_code"] = sub_tier_agency.cgac.cgac_code
            job_data["d2_submission"] = True

            # convert submission start/end dates from the request into Python date objects
            date_format = '%d/%m/%Y'
            try:
                job_data['reporting_start_date'] = datetime.strptime(job_data['reporting_start_date'],
                                                                     date_format).date()
                job_data['reporting_end_date'] = datetime.strptime(job_data['reporting_end_date'],
                                                                   date_format).date()
            except ValueError:
                raise ResponseException("Date must be provided as DD/MM/YYYY", StatusCode.CLIENT_ERROR, ValueError)

            # the front-end is doing date checks, but we'll also do a few server side to ensure everything is correct
            # when clients call the API directly
            if job_data.get('reporting_start_date') > job_data.get('reporting_end_date'):
                raise ResponseException("Submission start date {} is after the end date {}".format(
                        job_data.get('reporting_start_date'), job_data.get('reporting_end_date')),
                        StatusCode.CLIENT_ERROR)

            if not current_user_can('writer', job_data["cgac_code"]):
                raise ResponseException("User does not have permission to create jobs for this agency",
                                        StatusCode.PERMISSION_DENIED)

            submission = create_submission(g.user.user_id, job_data, None)
            sess.add(submission)
            sess.commit()
            sub_tier_affiliation = SubmissionSubTierAffiliation(submission_id=submission.submission_id,
                                                                sub_tier_agency_id=sub_tier_agency.sub_tier_agency_id)
            sess.add(sub_tier_affiliation)
            sess.commit()

            # build fileNameMap to be used in creating jobs
            self.build_file_map(request_params, ['detached_award'], response_dict, upload_files)

            self.create_response_dict_for_submission(upload_files, submission, False, response_dict, create_credentials)
            return JsonResponse.create(StatusCode.OK, response_dict)
        except (ValueError, TypeError, NotImplementedError) as e:
            return JsonResponse.error(e, StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            # call error route directly, status code depends on exception
            return JsonResponse.error(e, e.status)
        except Exception as e:
            # unexpected exception, this is a 500 server error
            return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)
        except:
            return JsonResponse.error(Exception("Failed to catch exception"), StatusCode.INTERNAL_ERROR)

    @staticmethod
    def check_detached_generation(job_id):
        """ Return information about file generation jobs

        Returns:
            Response object with keys job_id, status, file_type, url, message, start, and end.
        """
        sess = GlobalDB.db().session

        # We want to user first() here so we can see if the job is None so we can mark
        # the status as invalid to indicate that a status request is invoked for a job that
        # isn't created yet
        upload_job = sess.query(Job).filter_by(job_id=job_id).one_or_none()

        response_dict = {'job_id': job_id, 'status': '', 'file_type': '', 'message': '', 'url': '',
                         'start': '', 'end': ''}

        if upload_job is None:
            response_dict['status'] = 'invalid'
            response_dict['message'] = 'No generation job found with the specified ID'
            return JsonResponse.create(StatusCode.OK, response_dict)

        file_type = FILE_TYPE_DICT_LETTER[upload_job.file_type_id]
        response_dict["status"] = JOB_STATUS_DICT_ID[upload_job.job_status_id]
        response_dict["file_type"] = file_type
        response_dict["message"] = upload_job.error_message or ""
        if upload_job.filename is None:
            response_dict["url"] = "#"
        elif CONFIG_BROKER["use_aws"]:
            path, file_name = upload_job.filename.split("/")
            response_dict["url"] = S3UrlHandler().get_signed_url(path=path, file_name=file_name, bucket_route=None,
                                                                 method="GET")
        else:
            response_dict["url"] = upload_job.filename

        response_dict["start"] = upload_job.start_date.strftime(
            "%m/%d/%Y") if upload_job.start_date is not None else ""
        response_dict["end"] = upload_job.end_date.strftime("%m/%d/%Y") if upload_job.end_date is not None else ""

        return JsonResponse.create(StatusCode.OK, response_dict)

    @staticmethod
    def check_generation(submission, file_type):
        """ Return information about file generation jobs

        Returns:
            Response object with keys status, file_type, url, message.
            If file_type is D1 or D2, also includes start and end.
        """
        sess = GlobalDB.db().session

        upload_job = sess.query(Job).filter_by(
            submission_id=submission.submission_id,
            file_type_id=FILE_TYPE_DICT_LETTER_ID[file_type],
            job_type_id=JOB_TYPE_DICT['file_upload']
        ).one()

        if file_type in ["D1", "D2"]:
            validation_job = sess.query(Job).filter_by(
                submission_id=submission.submission_id,
                file_type_id=FILE_TYPE_DICT_LETTER_ID[file_type],
                job_type_id=JOB_TYPE_DICT['csv_record_validation']
            ).one()
        else:
            validation_job = None
        response_dict = {
            'status': map_generate_status(upload_job, validation_job),
            'file_type': file_type,
            'message': upload_job.error_message or ""
        }
        if upload_job.filename is None:
            response_dict["url"] = "#"
        elif CONFIG_BROKER["use_aws"]:
            path, file_name = upload_job.filename.split("/")
            response_dict["url"] = S3UrlHandler().get_signed_url(path=path, file_name=file_name,
                                                                 bucket_route=None, method="GET")
        else:
            response_dict["url"] = upload_job.filename

        # Pull start and end from jobs table if D1 or D2
        if file_type in ["D1", "D2"]:
            response_dict["start"] = upload_job.start_date.strftime("%m/%d/%Y") if upload_job.start_date else ""
            response_dict["end"] = upload_job.end_date.strftime("%m/%d/%Y") if upload_job.end_date else ""

        return JsonResponse.create(StatusCode.OK, response_dict)

    @staticmethod
    def submit_detached_file(submission):
        """ Submits the FABS upload file associated with the submission ID """
        # Check to make sure it's a d2 submission
        if not submission.d2_submission:
            raise ResponseException("Submission is not a FABS submission", StatusCode.CLIENT_ERROR)

        # Check to make sure it isn't already a published submission
        if submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished']:
            raise ResponseException("Submission has already been published", StatusCode.CLIENT_ERROR)

        # if it's an unpublished FABS submission, we can start the process
        sess = GlobalDB.db().session
        submission_id = submission.submission_id

        try:
            # get all valid lines for this submission
            query = sess.query(DetachedAwardFinancialAssistance).\
                filter_by(is_valid=True, submission_id=submission_id).all()

            for row in query:
                # if it is not a delete row
                if row.correction_late_delete_ind is None or row.correction_late_delete_ind.upper() != "D":
                    # remove all keys in the row that are not in the intermediate table
                    temp_obj = row.__dict__
                    temp_obj.pop('detached_award_financial_assistance_id', None)
                    temp_obj.pop('submission_id', None)
                    temp_obj.pop('job_id', None)
                    temp_obj.pop('row_number', None)
                    temp_obj.pop('is_valid', None)
                    temp_obj.pop('_sa_instance_state', None)
                    # if it is a new row, just insert it
                    if row.correction_late_delete_ind is None:
                        new_row = PublishedAwardFinancialAssistance(**temp_obj)
                        sess.add(new_row)
                    # if it's a correction row, check if it exists
                    else:
                        check_row = sess.query(PublishedAwardFinancialAssistance).\
                            filter_by(fain=row.fain, uri=row.uri,
                                      awarding_sub_tier_agency_c=row.awarding_sub_tier_agency_c,
                                      award_modification_amendme=row.award_modification_amendme).one_or_none()
                        # if the row exists, update the existing row
                        if check_row:
                            sess.query(PublishedAwardFinancialAssistance).\
                                filter_by(fain=row.fain, uri=row.uri,
                                          awarding_sub_tier_agency_c=row.awarding_sub_tier_agency_c,
                                          award_modification_amendme=row.award_modification_amendme).\
                                update(temp_obj, synchronize_session=False)
                        # if the row doesn't exist, add a new one
                        else:
                            new_row = PublishedAwardFinancialAssistance(**temp_obj)
                            sess.add(new_row)
                # if it is a delete row, delete the associated row from the list
                else:
                    sess.query(PublishedAwardFinancialAssistance).\
                        filter_by(fain=row.fain, uri=row.uri,
                                  awarding_sub_tier_agency_c=row.awarding_sub_tier_agency_c,
                                  award_modification_amendme=row.award_modification_amendme).delete()
            sess.commit()
        except Exception as e:
            # rollback the changes if there are any errors. We want to submit everything together
            sess.rollback()
            return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)

        sess.query(Submission).filter_by(submission_id=submission_id).\
            update({"publish_status_id": PUBLISH_STATUS_DICT['published']}, synchronize_session=False)
        response_dict = {"submission_id": submission_id}
        return JsonResponse.create(StatusCode.OK, response_dict)

    def get_protected_files(self):
        """ Returns a set of urls to protected files on the help page """
        response = {}
        if self.isLocal:
            response["urls"] = {}
            return JsonResponse.create(StatusCode.CLIENT_ERROR, response)

        response["urls"] = self.s3manager.get_file_urls(bucket_name=CONFIG_BROKER["static_files_bucket"],
                                                        path=CONFIG_BROKER["help_files_path"])
        return JsonResponse.create(StatusCode.OK, response)

    def complete_generation(self, generation_id, file_type=None):
        """ For files D1 and D2, the API uses this route as a callback to load the generated file.
        Requires an 'href' key in the request that specifies the URL of the file to be downloaded

        Args:
            generation_id - Unique key stored in file_generation_task table, used in callback to
                identify which submission this file is for.
            file_type - the type of file to be generated, D1 or D2. Only used when calling
                complete_generation for local development

        """
        sess = GlobalDB.db().session
        try:
            if generation_id is None:
                raise ResponseException("Must include a generation ID", StatusCode.CLIENT_ERROR)

            if not self.isLocal:
                # Pull url from request
                request_dict = RequestDictionary.derive(self.request)
                logger.debug('Request content => %s', request_dict)

                if 'href' not in request_dict:
                    raise ResponseException("Request must include href key with URL of D file", StatusCode.CLIENT_ERROR)

                url = request_dict['href']
                logger.debug('Download URL => %s', url)
            else:
                if file_type == "D1":
                    url = CONFIG_SERVICES["d1_file_path"]
                else:
                    url = CONFIG_SERVICES["d2_file_path"]

            # Pull information based on task key
            logger.debug('Pulling information based on task key...')
            task = sess.query(FileGenerationTask).filter(FileGenerationTask.generation_task_key == generation_id).one()
            job = sess.query(Job).filter_by(job_id=task.job_id).one()
            logger.debug('Loading D file...')
            result = self.load_d_file(url, job.filename, job.original_filename, job.job_id, self.isLocal)
            logger.debug('Load D file result => %s', result)
            return JsonResponse.create(StatusCode.OK, {"message": "File loaded successfully"})
        except ResponseException as e:
            return JsonResponse.error(e, e.status)
        except NoResultFound:
            # Did not find file generation task
            return JsonResponse.error(ResponseException("Generation task key not found", StatusCode.CLIENT_ERROR),
                                      StatusCode.CLIENT_ERROR)

    @staticmethod
    def get_d_file_url(task_key, file_type_name, cgac_code, start_date, end_date):
        """ Compiles the URL to be called in order to generate the D files """
        callback = "{}://{}:{}/v1/complete_generation/{}/".format(CONFIG_SERVICES["protocol"],
                                                                  CONFIG_SERVICES["broker_api_host"],
                                                                  CONFIG_SERVICES["broker_api_port"], task_key)
        logger.debug('Callback URL for %s: %s', FILE_TYPE_DICT_LETTER[FILE_TYPE_DICT[file_type_name]], callback)
        url = CONFIG_BROKER["".join([file_type_name, "_url"])].format(cgac_code, start_date, end_date, callback)
        return url

    @staticmethod
    def create_generation_task(job_id):
        sess = GlobalDB.db().session
        task_key = uuid4()
        task = FileGenerationTask(generation_task_key=task_key, job_id=job_id)
        sess.add(task)
        sess.commit()
        return task.generation_task_key

    def add_generation_job_info(self, file_type_name, job=None, dates=None):
        # if job is None, that means the info being added is for detached d file generation
        sess = GlobalDB.db().session
        user_id = g.user.user_id

        timestamped_name = S3UrlHandler.get_timestamped_filename(
            CONFIG_BROKER["".join([str(file_type_name), "_file_name"])])
        if self.isLocal:
            upload_file_name = "".join([CONFIG_BROKER['broker_files'], timestamped_name])
        else:
            upload_file_name = "".join([str(user_id), "/", timestamped_name])

        if job is None:
            job = Job(job_type_id=JOB_TYPE_DICT['file_upload'], user_id=user_id,
                      file_type_id=FILE_TYPE_DICT[file_type_name], start_date=dates['start_date'],
                      end_date=dates['end_date'])
            sess.add(job)

        # This will update the reference so no need to return the job, just the upload and timestamped file names
        job.filename = upload_file_name
        job.original_filename = timestamped_name
        job.job_status_id = JOB_STATUS_DICT["running"]
        sess.commit()

        return job

    def build_file_map(self, request_params, file_type_list, response_dict, upload_files, existing_submission=False):
        """ build fileNameMap to be used in creating jobs """
        for file_type in file_type_list:
            # if file_type not included in request, and this is an update to an existing submission, skip it
            if not request_params.get(file_type):
                if existing_submission:
                    continue
                # this is a new submission, all files are required
                raise ResponseException("Must include all required files for new submission", StatusCode.CLIENT_ERROR)

            file_name = request_params.get(file_type)
            if file_name:
                if not self.isLocal:
                    upload_name = "{}/{}".format(
                        g.user.user_id,
                        S3UrlHandler.get_timestamped_filename(file_name)
                    )
                else:
                    upload_name = file_name

                response_dict[file_type + "_key"] = upload_name
                upload_files.append(FileHandler.UploadFile(
                    file_type=file_type,
                    upload_name=upload_name,
                    file_name=file_name,
                    file_letter=FILE_TYPE_DICT_LETTER[FILE_TYPE_DICT[file_type]]
                ))

    def create_response_dict_for_submission(self, upload_files, submission, existing_submission, response_dict,
                                            create_credentials):
        file_job_dict = create_jobs(upload_files, submission, existing_submission)
        for file_type in file_job_dict.keys():
            if "submission_id" not in file_type:
                response_dict[file_type + "_id"] = file_job_dict[file_type]
        if create_credentials and not self.isLocal:
            self.s3manager = S3UrlHandler(CONFIG_BROKER["aws_bucket"])
            response_dict["credentials"] = self.s3manager.get_temporary_credentials(g.user.user_id)
        else:
            response_dict["credentials"] = {"AccessKeyId": "local", "SecretAccessKey": "local",
                                            "SessionToken": "local", "Expiration": "local"}

        response_dict["submission_id"] = file_job_dict["submission_id"]
        if self.isLocal:
            response_dict["bucket_name"] = CONFIG_BROKER["broker_files"]
        else:
            response_dict["bucket_name"] = CONFIG_BROKER["aws_bucket"]

    @staticmethod
    def restart_validation(submission):
        # update all validation jobs to "ready"
        sess = GlobalDB.db().session
        initial_file_types = [FILE_TYPE_DICT['appropriations'], FILE_TYPE_DICT['program_activity'],
                              FILE_TYPE_DICT['award_financial']]

        jobs = sess.query(Job).filter(Job.submission_id == submission.submission_id).all()

        # set all jobs to their initial status of "waiting"
        for job in jobs:
            job.job_status_id = JOB_STATUS_DICT['waiting']

        # update upload jobs to "running", only for files A, B, and C
        upload_jobs = [job for job in jobs if job.job_type_id in [JOB_TYPE_DICT['file_upload']] and
                       job.file_type_id in initial_file_types]

        for job in upload_jobs:
            job.job_status_id = JOB_STATUS_DICT['running']
        sess.commit()

        # call finalize job for the upload jobs for files A, B, and C which will kick off the rest of
        for job in upload_jobs:
            FileHandler.finalize(job.job_id)

        return JsonResponse.create(StatusCode.OK, {"message": "Success"})


def narratives_for_submission(submission):
    """Fetch narratives for this submission, indexed by file letter"""
    sess = GlobalDB.db().session
    result = {letter: '' for letter in FILE_TYPE_DICT_LETTER.values()}
    narratives = sess.query(SubmissionNarrative).\
        filter_by(submission_id=submission.submission_id)
    for narrative in narratives:
        letter = FILE_TYPE_DICT_LETTER[narrative.file_type_id]
        result[letter] = narrative.narrative
    return JsonResponse.create(StatusCode.OK, result)


def update_narratives(submission, narratives_json):
    """Clear existing narratives and replace them with the provided set. We
    assume narratives_json contains non-empty strings (i.e. that it's been
    cleaned)"""
    sess = GlobalDB.db().session
    sess.query(SubmissionNarrative).\
        filter_by(submission_id=submission.submission_id).\
        delete(synchronize_session='fetch')     # fetch just in case
    narratives = []
    for file_type_id, letter in FILE_TYPE_DICT_LETTER.items():
        if letter in narratives_json:
            narratives.append(SubmissionNarrative(
                submission_id=submission.submission_id,
                file_type_id=file_type_id,
                narrative=narratives_json[letter]
            ))
    sess.add_all(narratives)
    sess.commit()

    return JsonResponse.create(StatusCode.OK, {})


def _split_csv(string):
    """Split string into a list, excluding empty strings"""
    if string is None:
        return []
    return [n.strip() for n in string.split(',') if n]


def job_to_dict(job):
    """Convert a Job model into a dictionary, ready to be serialized as JSON"""
    sess = GlobalDB.db().session

    job_info = {
        'job_id': job.job_id,
        'job_status': job.job_status_name,
        'job_type': job.job_type_name,
        'filename': job.original_filename,
        'file_size': job.file_size,
        'number_of_rows': job.number_of_rows,
        'file_type': job.file_type_name or '',
    }

    # @todo replace with relationships
    file_results = sess.query(File).filter_by(job_id=job.job_id).one_or_none()
    if file_results is None:
        # Job ID not in error database, probably did not make it to
        # validation, or has not yet been validated
        job_info.update(
            file_status="",
            error_type="",
            error_data=[],
            warning_data=[],
            missing_headers=[],
            duplicated_headers=[],
        )
    else:
        # If job ID was found in file, we should be able to get header error
        # lists and file data. Get string of missing headers and parse as a
        # list
        job_info['file_status'] = file_results.file_status_name
        job_info['missing_headers'] = _split_csv(file_results.headers_missing)
        job_info["duplicated_headers"] = _split_csv(
            file_results.headers_duplicated)
        job_info["error_type"] = get_error_type(job.job_id)
        job_info["error_data"] = get_error_metrics_by_job_jd(
            job.job_id, job.job_type_name == 'validation',
            severity_id=RULE_SEVERITY_DICT['fatal']
        )
        job_info["warning_data"] = get_error_metrics_by_job_jd(
            job.job_id, job.job_type_name == 'validation',
            severity_id=RULE_SEVERITY_DICT['warning']
        )
    return job_info


def reporting_date(submission):
    """Format submission reporting date"""
    if submission.is_quarter_format:
        return 'Q{}/{}'.format(submission.reporting_fiscal_period // 3,
                               submission.reporting_fiscal_year)
    else:
        return submission.reporting_start_date.strftime("%m/%Y")


def submission_to_dict_for_status(submission):
    """Convert a Submission model into a dictionary, ready to be serialized as
    JSON for the get_status function"""
    sess = GlobalDB.db().session

    number_of_rows = sess.query(func.sum(Job.number_of_rows)).\
        filter_by(submission_id=submission.submission_id).\
        scalar() or 0

    # @todo replace with a relationship
    cgac = sess.query(CGAC).\
        filter_by(cgac_code=submission.cgac_code).one_or_none()
    if cgac:
        agency_name = cgac.agency_name
    else:
        agency_name = ''

    relevant_job_types = (JOB_TYPE_DICT['csv_record_validation'],
                          JOB_TYPE_DICT['validation'])
    relevant_jobs = sess.query(Job).filter(
        Job.submission_id == submission.submission_id,
        Job.job_type_id.in_(relevant_job_types)
    )

    revalidation_threshold = sess.query(RevalidationThreshold).one_or_none()
    last_validated = get_last_validated_date(submission.submission_id)

    return {
        'cgac_code': submission.cgac_code,
        'agency_name': agency_name,
        'created_on': submission.created_at.strftime('%m/%d/%Y'),
        'number_of_errors': submission.number_of_errors,
        'number_of_rows': number_of_rows,
        'last_updated': submission.updated_at.strftime("%Y-%m-%dT%H:%M:%S"),
        'last_validated': last_validated,
        'revalidation_threshold':
            revalidation_threshold.revalidation_date.strftime('%m/%d/%Y') if revalidation_threshold else '',
        # Broker allows submission for a single quarter or a single month,
        # so reporting_period start and end dates reported by check_status
        # are always equal
        'reporting_period_start_date': reporting_date(submission),
        'reporting_period_end_date': reporting_date(submission),
        'jobs': [job_to_dict(job) for job in relevant_jobs],
        'publish_status': submission.publish_status.name
    }


def get_status(submission):
    """ Get description and status of all jobs in the submission specified in request object

    Returns:
        A flask response object to be sent back to client, holds a JSON where each job ID has a dictionary holding
        file_type, job_type, status, and filename
    """
    try:
        return JsonResponse.create(StatusCode.OK, submission_to_dict_for_status(submission))
    except ResponseException as e:
        return JsonResponse.error(e, e.status)
    except Exception as e:
        # Unexpected exception, this is a 500 server error
        return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)


def get_error_metrics(submission):
    """Returns an Http response object containing error information for every
    validation job in specified submission """
    sess = GlobalDB.db().session
    return_dict = {}
    try:
        jobs = sess.query(Job).filter_by(submission_id=submission.submission_id)
        for job in jobs:
            if job.job_type.name == 'csv_record_validation':
                file_type = job.file_type.name
                data_list = get_error_metrics_by_job_jd(job.job_id)
                return_dict[file_type] = data_list
        return JsonResponse.create(StatusCode.OK, return_dict)
    except (ValueError, TypeError) as e:
        return JsonResponse.error(e, StatusCode.CLIENT_ERROR)
    except ResponseException as e:
        return JsonResponse.error(e, e.status)
    except Exception as e:
        # Unexpected exception, this is a 500 server error
        return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)


def list_submissions(page, limit, certified, sort='modified', order='desc'):
    """ List submission based on current page and amount to display. If provided, filter based on
    certification status """
    sess = GlobalDB.db().session

    offset = limit * (page - 1)

    cgac_codes = [aff.cgac.cgac_code for aff in g.user.affiliations]
    query = sess.query(Submission).filter_by(d2_submission=False)
    if not g.user.website_admin:
        query = query.filter(sa.or_(Submission.cgac_code.in_(cgac_codes),
                                    Submission.user_id == g.user.user_id))
    if certified != 'mixed':
        if certified == 'true':
            query = query.filter(Submission.publish_status_id == PUBLISH_STATUS_DICT['published'])
        else:
            query = query.filter(Submission.publish_status_id != PUBLISH_STATUS_DICT['published'])

    arr = [serialize_submission(s) for s in query]

    options = {
        'modified': 'last_modified',
        'reporting': 'reporting_start_date',
        'status': 'status',
        'agency': 'agency'
    }

    if not options.get(sort):
        sort = 'modified'

    if sort == 'submitted_by':
        arr.sort(key=lambda x: x.get('user').get('name'))
    else:
        arr.sort(key=lambda x: x.get(options.get(sort)))

    if order == 'desc':
        arr.reverse()

    return JsonResponse.create(StatusCode.OK, {
        "submissions": arr[offset:offset+limit],
        "total": query.count()
    })


def serialize_submission(submission):
    """Convert the provided submission into a dictionary in a schema the
    frontend expects"""
    sess = GlobalDB.db().session
    # @todo these should probably be part of the query rather than spawning n
    # queries
    total_size = sess.query(func.sum(Job.file_size)).\
        filter_by(submission_id=submission.submission_id).\
        scalar() or 0

    status = get_submission_status(submission)
    if submission.user_id is None:
        submission_user_name = "No user"
    else:
        submission_user_name = sess.query(User).filter_by(user_id=submission.user_id).one().name

    cgac = sess.query(CGAC).\
        filter_by(cgac_code=submission.cgac_code).one_or_none()

    return {
        "submission_id": submission.submission_id,
        "last_modified": submission.updated_at.strftime('%Y-%m-%d'),
        "size": total_size,
        "status": status,
        "agency": cgac.agency_name if cgac else 'N/A',
        # @todo why are these a different format?
        "reporting_start_date": str(submission.reporting_start_date),
        "reporting_end_date": str(submission.reporting_end_date),
        "user": {"user_id": submission.user_id,
                 "name": submission_user_name}
    }


def submission_report_url(submission, warning, file_type, cross_type):
    """ Gets the signed URL for the specified file """
    file_name = report_file_name(
        submission.submission_id, warning, file_type, cross_type)
    if CONFIG_BROKER['local']:
        url = os.path.join(CONFIG_BROKER['broker_files'], file_name)
    else:
        url = S3UrlHandler().get_signed_url("errors", file_name, method="GET")
    return JsonResponse.create(StatusCode.OK, {"url": url})


def get_cross_report_key(source_type, target_type, is_warning=False):
    """ Generate a key for cross-file error reports """
    if is_warning:
        return "cross_warning_{}-{}".format(source_type, target_type)
    else:
        return "cross_{}-{}".format(source_type, target_type)


def submission_error(submission_id, file_type):
    """ Check that submission exists and user has permission to it

    Args:
        submission_id:  ID of submission to check
        file_type: file type that has been requested

    Returns:
        A JsonResponse if there's an error, None otherwise
    """
    sess = GlobalDB.db().session

    submission = sess.query(Submission).filter_by(submission_id=submission_id).one_or_none()
    if submission is None:
        # Submission does not exist, change to 400 in this case since
        # route call specified a bad ID
        response_dict = {
            "message": "Submission does not exist",
            "file_type": file_type,
            "url": "#",
            "status": "failed"
        }
        if file_type in ('D1', 'D2'):
            # Add empty start and end dates
            response_dict["start"] = ""
            response_dict["end"] = ""
        return JsonResponse.error(NoResultFound, StatusCode.CLIENT_ERROR, **response_dict)

    if not current_user_can_on_submission('writer', submission):
        response_dict = {
            "message": "User does not have permission to view that submission",
            "file_type": file_type,
            "url": "#",
            "status": "failed"
        }
        if file_type in ('D1', 'D2'):
            # Add empty start and end dates
            response_dict["start"] = ""
            response_dict["end"] = ""
        return JsonResponse.create(StatusCode.PERMISSION_DENIED, response_dict)


def get_xml_response_content(api_url):
    """ Retrieve XML Response from the provided API url """
    result = requests.get(api_url, verify=False, timeout=120).text
    logger.debug('Result for %s: %s', api_url, result)
    return result


def get_lines_from_csv(file_path):
    """ Retrieve all lines from specified CSV file """
    lines = []
    with open(file_path) as file:
        for line in reader(file):
            lines.append(line)
    return lines


def map_generate_status(upload_job, validation_job=None):
    """ Maps job status to file generation statuses expected by frontend """
    sess = GlobalDB.db().session
    upload_status = upload_job.job_status.name
    if validation_job is None:
        errors_present = False
        validation_status = None
    else:
        validation_status = validation_job.job_status.name
        if check_number_of_errors_by_job_id(validation_job.job_id) > 0:
            errors_present = True
        else:
            errors_present = False

    response_status = FileHandler.STATUS_MAP[upload_status]
    if response_status == "failed" and upload_job.error_message is None:
        # Provide an error message if none present
        upload_job.error_message = "Upload job failed without error message"

    if validation_job is None:
        # No validation job, so don't need to check it
        sess.commit()
        return response_status

    if response_status == "finished":
        # Check status of validation job if present
        response_status = FileHandler.VALIDATION_STATUS_MAP[validation_status]
        if response_status == "finished" and errors_present:
            # If validation completed with errors, mark as failed
            response_status = "failed"
            upload_job.error_message = "Validation completed but row-level errors were found"

    if response_status == "failed":
        if upload_job.error_message is None and validation_job.error_message is None:
            if validation_status == "invalid":
                upload_job.error_message = "Generated file had file-level errors"
            else:
                upload_job.error_message = "Validation job had an internal error"

        elif upload_job.error_message is None:
            upload_job.error_message = validation_job.error_message
    sess.commit()
    return response_status
