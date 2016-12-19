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
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import NoResultFound
from werkzeug import secure_filename

from dataactbroker.permissions import (
    current_user_can, current_user_can_on_submission)
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.errorModels import File
from dataactcore.models.jobModels import (
    FileGenerationTask, Job, Submission, SubmissionNarrative)
from dataactcore.models.userModel import User
from dataactcore.models.lookups import (
    FILE_TYPE_DICT, FILE_TYPE_DICT_LETTER, FILE_TYPE_DICT_LETTER_ID,
    JOB_STATUS_DICT, JOB_TYPE_DICT, RULE_SEVERITY_DICT, FILE_TYPE_DICT_ID, JOB_STATUS_DICT_ID)
from dataactcore.utils.jobQueue import generate_e_file, generate_f_file
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.report import (get_report_path, get_cross_report_name,
                                      get_cross_warning_report_name, get_cross_file_pairs)
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.stringCleaner import StringCleaner
from dataactcore.interfaces.function_bag import (
    checkNumberOfErrorsByJobId, create_jobs, create_submission,
    getErrorMetricsByJobId, getErrorType, get_submission_status,
    mark_job_status, run_job_checks
)
from dataactvalidator.filestreaming.csv_selection import write_csv

logger = logging.getLogger(__name__)


class FileHandler:
    """ Responsible for all tasks relating to file upload

    Static fields:
    FILE_TYPES -- list of file labels that can be included

    Instance fields:
    request -- A flask request object, comes with the request
    s3manager -- instance of s3UrlHandler, manages calls to S3
    """

    FILE_TYPES = ["appropriations","award_financial","program_activity"]
    EXTERNAL_FILE_TYPES = ["award", "award_procurement", "awardee_attributes", "sub_award"]
    VALIDATOR_RESPONSE_FILE = "validatorResponse"
    STATUS_MAP = {"waiting":"invalid", "ready":"invalid", "running":"waiting", "finished":"finished", "invalid":"failed", "failed":"failed"}
    VALIDATION_STATUS_MAP = {"waiting":"waiting", "ready":"waiting", "running":"waiting", "finished":"finished", "failed":"failed", "invalid":"failed"}

    UploadFile = namedtuple('UploadFile', ['file_type', 'upload_name', 'file_name', 'file_letter'])

    def __init__(self, request, isLocal=False, serverPath=""):
        """ Create the File Handler

        Arguments:
            request - HTTP request object for this route
            isLocal - True if this is a local installation that will not use AWS or Smartronix
            serverPath - If isLocal is True, this is used as the path to local files
        """
        self.request = request
        self.isLocal = isLocal
        self.serverPath = serverPath
        self.s3manager = s3UrlHandler()

    def getErrorReportURLsForSubmission(self, is_warning = False):
        """
        Gets the Signed URLs for download based on the submissionId
        """
        sess = GlobalDB.db().session
        try:
            self.s3manager = s3UrlHandler()
            safe_dictionary = RequestDictionary(self.request)
            submission_id = safe_dictionary.getValue("submission_id")
            response_dict ={}
            jobs = sess.query(Job).filter_by(submission_id=submission_id)
            for job in jobs:
                if job.job_type.name == 'csv_record_validation':
                    if is_warning:
                        report_name = get_report_path(job, 'warning')
                        key = 'job_{}_warning_url'.format(job.job_id)
                    else:
                        report_name = get_report_path(job, 'error')
                        key = 'job_{}_error_url'.format(job.job_id)
                    if not self.isLocal:
                        response_dict[key] = self.s3manager.getSignedUrl("errors", report_name, method="GET")
                    else:
                        path = os.path.join(self.serverPath, report_name)
                        response_dict[key] = path

            # For each pair of files, get url for the report
            for c in get_cross_file_pairs():
                first_file = c[0]
                second_file = c[1]
                if is_warning:
                    report_name = get_cross_warning_report_name(
                        submission_id, first_file.name, second_file.name)
                else:
                    report_name = get_cross_report_name(
                        submission_id, first_file.name, second_file.name)
                if self.isLocal:
                    report_path = os.path.join(self.serverPath, report_name)
                else:
                    report_path = self.s3manager.getSignedUrl("errors", report_name, method="GET")
                # Assign to key based on source and target
                response_dict[self.getCrossReportKey(first_file.name, second_file.name, is_warning)] = report_path

            return JsonResponse.create(StatusCode.OK, response_dict)

        except ResponseException as e:
            return JsonResponse.error(e, StatusCode.CLIENT_ERROR)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)

    def get_signed_url_for_submission_file(self, submission):
        """ Gets the signed URL for the specified file """
        try:
            sess = GlobalDB.db().session
            self.s3manager = s3UrlHandler()
            safe_dictionary = RequestDictionary.derive(self.request)
            file_name = safe_dictionary["file"] + ".csv"

            response_dict = {}
            if self.isLocal:
                response_dict["url"] = os.path.join(self.serverPath, file_name)
            else:
                response_dict["url"] = self.s3manager.getSignedUrl("errors", file_name, method="GET")
            return JsonResponse.create(StatusCode.OK, response_dict)
        except ResponseException as e:
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)

    def getCrossReportKey(self,sourceType,targetType,isWarning = False):
        """ Generate a key for cross-file error reports """
        if isWarning:
            return "cross_warning_{}-{}".format(sourceType,targetType)
        else:
            return "cross_{}-{}".format(sourceType,targetType)

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
            response_dict= {}
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
                existing_submission
                and not current_user_can_on_submission(
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
            for file_type in FileHandler.FILE_TYPES :
                # if filetype not included in request, and this is an update to an existing submission, skip it
                if not request_params.get(file_type):
                    if existing_submission:
                        continue
                    # this is a new submission, all files are required
                    raise ResponseException("Must include all files for new submission", StatusCode.CLIENT_ERROR)

                filename = request_params.get(file_type)
                if filename:
                    if not self.isLocal:
                        upload_name = "{}/{}".format(
                            g.user.user_id,
                            s3UrlHandler.getTimestampedFilename(filename)
                        )
                    else:
                        upload_name = filename
                    response_dict[file_type+"_key"] = upload_name
                    upload_files.append(FileHandler.UploadFile(
                        file_type=file_type,
                        upload_name=upload_name,
                        file_name=filename,
                        file_letter=FILE_TYPE_DICT_LETTER[FILE_TYPE_DICT[file_type]]
                    ))

            if not upload_files and existing_submission:
                raise ResponseException("Must include at least one file for an existing submission",
                                        StatusCode.CLIENT_ERROR)
            if not existing_submission:
                # don't add external files to existing submission
                for ext_file_type in FileHandler.EXTERNAL_FILE_TYPES:
                    filename = CONFIG_BROKER["".join([ext_file_type,"_file_name"])]

                    if not self.isLocal:
                        upload_name = "{}/{}".format(
                            g.user.user_id,
                            s3UrlHandler.getTimestampedFilename(filename)
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

            file_job_dict = create_jobs(upload_files, submission, existing_submission)
            for file_type in file_job_dict.keys():
                if not "submission_id" in file_type:
                    response_dict[file_type+"_id"] = file_job_dict[file_type]
            if create_credentials and not self.isLocal:
                self.s3manager = s3UrlHandler(CONFIG_BROKER["aws_bucket"])
                response_dict["credentials"] = self.s3manager.getTemporaryCredentials(g.user.user_id)
            else :
                response_dict["credentials"] ={"AccessKeyId" : "local","SecretAccessKey" :"local","SessionToken":"local" ,"Expiration" :"local"}

            response_dict["submission_id"] = file_job_dict["submission_id"]
            if self.isLocal:
                response_dict["bucket_name"] = CONFIG_BROKER["broker_files"]
            else:
                response_dict["bucket_name"] = CONFIG_BROKER["aws_bucket"]
            return JsonResponse.create(StatusCode.OK,response_dict)
        except (ValueError , TypeError, NotImplementedError) as e:
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            # call error route directly, status code depends on exception
            return JsonResponse.error(e,e.status)
        except Exception as e:
            # unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)
        except:
            return JsonResponse.error(Exception("Failed to catch exception"),StatusCode.INTERNAL_ERROR)

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

    def finalize(self):
        """ Set upload job in job tracker database to finished, allowing dependent jobs to be started

        Flask request should include key "upload_id", which holds the job_id for the file_upload job

        Returns:
        A flask response object, if successful just contains key "success" with value True, otherwise value is False
        """
        sess = GlobalDB.db().session
        response_dict = {}
        try:
            input_dictionary = RequestDictionary(self.request)
            job_id = input_dictionary.getValue("upload_id")

            # Compare user ID with user who submitted job, if no match return 400
            job = sess.query(Job).filter_by(job_id = job_id).one()
            submission = sess.query(Submission).filter_by(submission_id = job.submission_id).one()
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

        except ( ValueError , TypeError ) as e:
            return JsonResponse.error(e, StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            return JsonResponse.error(e, e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)

    def submission_error(self, submission_id, file_type):
        """ Check that submission exists and user has permission to it

        Args:
            submission_id:  ID of submission to check
            file_type: file type that has been requested

        Returns:
            A JsonResponse if there's an error, None otherwise
        """
        sess = GlobalDB.db().session

        submission = sess.query(Submission).\
            filter_by(submission_id=submission_id).one_or_none()
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
            return JsonResponse.error(
                NoResultFound, StatusCode.CLIENT_ERROR, **response_dict)

        if not current_user_can_on_submission('writer', submission):
            response_dict = {
                "message": ("User does not have permission to view that "
                            "submission"),
                "file_type": file_type,
                "url": "#",
                "status": "failed"
            }
            if file_type in ('D1', 'D2'):
                # Add empty start and end dates
                response_dict["start"] = ""
                response_dict["end"] = ""
            return JsonResponse.create(StatusCode.PERMISSION_DENIED,
                                       response_dict)

    def uploadFile(self):
        """ Saves a file and returns the saved path.  Should only be used for local installs. """
        try:
            if(self.isLocal):
                uploadedFile = request.files['file']
                if(uploadedFile):
                    seconds = int((datetime.utcnow()-datetime(1970,1,1)).total_seconds())
                    filename = "".join([str(seconds),"_", secure_filename(uploadedFile.filename)])
                    path = os.path.join(self.serverPath, filename)
                    uploadedFile.save(path)
                    returnDict = {"path":path}
                    return JsonResponse.create(StatusCode.OK,returnDict)
                else:
                    raise ResponseException("Failure to read file",
                                            StatusCode.CLIENT_ERROR)
            else :
                raise ResponseException("Route Only Valid For Local Installs",
                                        StatusCode.CLIENT_ERROR)
        except ( ValueError , TypeError ) as e:
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)

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
                requestDict = RequestDictionary(self.request)
                start_date = requestDict.getValue("start")
                end_date = requestDict.getValue("end")

                if not (StringCleaner.isDate(start_date)
                            and StringCleaner.isDate(end_date)):
                    raise ResponseException(
                        "Start or end date cannot be parsed into a date",
                        StatusCode.CLIENT_ERROR
                    )
            elif file_type not in ["E","F"]:
                raise ResponseException(
                    "File type must be either D1, D2, E or F",
                    StatusCode.CLIENT_ERROR
                )
        except ResponseException as e:
            return False, JsonResponse.error(
                e, e.status, file_type=file_type, status='failed')

        submission = sess.query(Submission).filter_by(submission_id=job.submission_id).one()
        cgac_code = submission.cgac_code

        # Generate and upload file to S3
        job = self.add_generation_job_info(file_type_name=file_type_name, job=job)
        upload_file_name, timestamped_name = job.filename, job.original_filename

        if file_type in ["D1", "D2"]:
            logger.debug('Adding job info for job id of %s', job.job_id)
            return self.add_job_info_for_d_file(upload_file_name, timestamped_name, submission.submission_id, file_type, file_type_name, start_date, end_date, cgac_code, job)
        elif file_type == 'E':
            generate_e_file.delay(
                submission.submission_id, job.job_id, timestamped_name,
                upload_file_name, self.isLocal)
        elif file_type == 'F':
            generate_f_file.delay(
                submission.submission_id, job.job_id, timestamped_name,
                upload_file_name, self.isLocal)

        return True, None

    def add_job_info_for_d_file(self, upload_file_name, timestamped_name, submission_id, file_type, file_type_name, start_date, end_date, cgac_code, job):
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
            job.start_date = datetime.strptime(start_date,"%m/%d/%Y").date()
            job.end_date = datetime.strptime(end_date,"%m/%d/%Y").date()
            val_job.start_date = datetime.strptime(start_date,"%m/%d/%Y").date()
            val_job.end_date = datetime.strptime(end_date,"%m/%d/%Y").date()
        except ValueError as e:
            # Date was not in expected format
            exc = ResponseException(str(e),StatusCode.CLIENT_ERROR,ValueError)
            return False, JsonResponse.error(exc, exc.status, url = "", start = "", end = "",  file_type = file_type)

        error = self.call_d_file_api(file_type_name, cgac_code, start_date, end_date, job)

        return not error, error

    def get_xml_response_content(self, api_url):
        """ Retrieve XML Response from the provided API url """
        result = requests.get(api_url, verify=False, timeout=120).text
        logger.debug('Result for %s: %s', api_url, result)
        return result

    def call_d_file_api(self, file_type_name, cgac_code, start_date, end_date, job):
        """ Call D file API, return True if results found, False otherwise """
        file_type = FILE_TYPE_DICT_LETTER[FILE_TYPE_DICT[file_type_name]]
        task_key = FileHandler.create_generation_task(job.job_id)

        if not self.isLocal:
            # Create file D API URL with dates and callback URL
            api_url = FileHandler.get_d_file_url(task_key, file_type_name, cgac_code, start_date, end_date)

            logger.debug('Calling D file API => %s', api_url)
            try:
                # Check for numFound = 0
                if "numFound='0'" in self.get_xml_response_content(api_url):
                    sess = GlobalDB.db().session
                    # If the call to the external API wasn't successful, mark the job as failed
                    job.error_message = "%s data unavailable for the specified date range" % file_type
                    job.job_status_id = JOB_STATUS_DICT['failed']
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

    def get_lines_from_csv(self, file_path):
        """ Retrieve all lines from specified CSV file """
        lines = []
        with open(file_path) as file:
            for line in reader(file):
                lines.append(line)
        return lines

    def load_d_file(self, url, upload_name, timestamped_name, job_id, isLocal):
        """ Pull D file from specified URL and write to S3 """
        sess = GlobalDB.db().session
        try:
            full_file_path = "".join([CONFIG_BROKER['d_file_storage_path'], timestamped_name])

            logger.debug('Downloading file...')
            if not self.download_file(full_file_path, url):
                # Error occurred while downloading file, mark job as failed and record error message
                mark_job_status(job_id, "failed")
                job = sess.query(Job).filter_by(job_id = job_id).one()
                file_type = job.file_type.name
                if file_type == "award":
                    source= "ASP"
                elif file_type == "award_procurement":
                    source = "FPDS"
                else:
                    source = "unknown source"
                job.error_message = "A problem occurred receiving data from {}".format(source)

                raise ResponseException(job.error_message, StatusCode.CLIENT_ERROR)
            lines = self.get_lines_from_csv(full_file_path)

            write_csv(timestamped_name, upload_name, isLocal, lines[0], lines[1:])

            logger.debug('Marking job id of %s', job_id)
            mark_job_status(job_id, "finished")
            return {"message": "Success", "file_name": timestamped_name}
        except Exception as e:
            logger.exception('Exception caught => %s', e)
            # Log the error
            JsonResponse.error(e,500)
            sess.query(Job).filter_by(job_id=job_id).one().error_message = str(e)
            mark_job_status(job_id, "failed")
            sess.commit()
            raise e

    def get_file_type(self):
        """ Pull information out of request object and return it

        Returns: tuple of submission ID and file type

        """
        request_dict = RequestDictionary.derive(self.request)
        if 'file_type' not in request_dict:
            raise ResponseException("Generate file route requires file_type",
                                    StatusCode.CLIENT_ERROR)

        return request_dict['file_type']

    def get_request_params_for_generate_detached(self):
        """ Pull information out of request object and return it
        for detached generation

        Returns: tuple of submission ID and file type

        """
        request_dict = RequestDictionary.derive(self.request)

        if not {'file_type', 'cgac_code', 'start', 'end'}.issubset(request_dict.keys()):
            raise ResponseException("Generate detached file route for D files requires file_type, cgac_code, "
                                    "start, and end",
                                    StatusCode.CLIENT_ERROR)
        return request_dict['file_type'], request_dict['cgac_code'], request_dict['start'], request_dict['end']

    def generate_file(self, submission_id):
        """ Start a file generation job for the specified file type """
        logger.debug('Starting D file generation')
        file_type = self.get_file_type()

        logger.debug('Submission ID = %s / File type = %s',
                            submission_id, file_type)

        sess = GlobalDB.db().session

        # Check permission to submission
        error = self.submission_error(submission_id, file_type)
        if error:
            return error

        job = sess.query(Job).filter_by(
            submission_id = submission_id,
            file_type_id = FILE_TYPE_DICT_LETTER_ID[file_type],
            job_type_id = JOB_TYPE_DICT['file_upload']
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
        return self.check_generation(submission)

    def generate_detached_file(self):
        """ Start a file generation job for the specified file type """
        logger.debug("Starting detached D file generation")

        file_type, cgac_code, start_date, end_date = self.get_request_params_for_generate_detached()

        # check file type
        if file_type not in ['D1', 'D2']:
            raise ResponseException("File type must be D1 or D2", StatusCode.CLIENT_ERROR)

        # check if date format is MM/DD/YYYY
        if not (StringCleaner.isDate(start_date) and StringCleaner.isDate(end_date)):
            raise ResponseException("Start or end date cannot be parsed into a date", StatusCode.CLIENT_ERROR)

        # add job info
        file_type_name = FILE_TYPE_DICT_ID[FILE_TYPE_DICT_LETTER_ID[file_type]]
        new_job = self.add_generation_job_info(file_type_name=file_type_name,
                                               dates={'start_date': start_date, 'end_date': end_date})

        result = self.call_d_file_api(file_type_name, cgac_code, start_date, end_date, new_job)

        # Return same response as check generation route
        return result or self.check_detached_generation(new_job.job_id)

    def check_detached_generation(self, job_id=None):
        """ Return information about file generation jobs

        Returns:
            Response object with keys job_id, status, file_type, url, message, start, and end.
        """

        if job_id is None:
            request_dict = RequestDictionary.derive(self.request)
            if 'job_id' not in request_dict:
                raise ResponseException("Check detached generation route requires job_id", StatusCode.CLIENT_ERROR)

            job_id = request_dict['job_id']

        sess = GlobalDB.db().session

        # We want to user first() here so we can see if the job is None so we can mark
        # the status as invalid to indicate that a status request is invoked for a job that
        # isn't created yet
        upload_job = sess.query(Job).filter_by(
            job_id=job_id).one_or_none()

        response_dict = {'job_id': job_id, 'status': '', 'file_type': '', 'message': '', 'url': '', 'start': '', 'end': ''}

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
            response_dict["url"] = s3UrlHandler().getSignedUrl(path=path, fileName=file_name, bucketRoute=None,
                                                              method="GET")
        else:
            response_dict["url"] = upload_job.filename

        response_dict["start"] = upload_job.start_date.strftime(
            "%m/%d/%Y") if upload_job.start_date is not None else ""
        response_dict["end"] = upload_job.end_date.strftime("%m/%d/%Y") if upload_job.end_date is not None else ""

        return JsonResponse.create(StatusCode.OK, response_dict)

    def check_generation(self, submission):
        """ Return information about file generation jobs

        Returns:
            Response object with keys status, file_type, url, message.
            If file_type is D1 or D2, also includes start and end.
        """
        sess = GlobalDB.db().session
        file_type = self.get_file_type()

        uploadJob = sess.query(Job).filter_by(
            submission_id=submission.submission_id,
            file_type_id=FILE_TYPE_DICT_LETTER_ID[file_type],
            job_type_id=JOB_TYPE_DICT['file_upload']
        ).one()

        if file_type in ["D1","D2"]:
            validationJob = sess.query(Job).filter_by(
                submission_id=submission.submission_id,
                file_type_id=FILE_TYPE_DICT_LETTER_ID[file_type],
                job_type_id=JOB_TYPE_DICT['csv_record_validation']
            ).one()
        else:
            validationJob = None
        responseDict = {}
        responseDict["status"] = self.mapGenerateStatus(uploadJob, validationJob)
        responseDict["file_type"] = file_type
        responseDict["message"] = uploadJob.error_message or ""
        if uploadJob.filename is None:
            responseDict["url"] = "#"
        elif CONFIG_BROKER["use_aws"]:
            path, file_name = uploadJob.filename.split("/")
            responseDict["url"] = s3UrlHandler().getSignedUrl(path=path, fileName=file_name, bucketRoute=None, method="GET")
        else:
            responseDict["url"] = uploadJob.filename

        # Pull start and end from jobs table if D1 or D2
        if file_type in ["D1","D2"]:
            responseDict["start"] = uploadJob.start_date.strftime("%m/%d/%Y") if uploadJob.start_date is not None else ""
            responseDict["end"] = uploadJob.end_date.strftime("%m/%d/%Y") if uploadJob.end_date is not None else ""

        return JsonResponse.create(StatusCode.OK, responseDict)

    def mapGenerateStatus(self, uploadJob, validationJob = None):
        """ Maps job status to file generation statuses expected by frontend """
        sess = GlobalDB.db().session
        uploadStatus = uploadJob.job_status.name
        if validationJob is None:
            errorsPresent = False
            validationStatus = None
        else:
            validationStatus = validationJob.job_status.name
            if checkNumberOfErrorsByJobId(validationJob.job_id) > 0:
                errorsPresent = True
            else:
                errorsPresent = False

        responseStatus = FileHandler.STATUS_MAP[uploadStatus]
        if responseStatus == "failed" and uploadJob.error_message is None:
            # Provide an error message if none present
            uploadJob.error_message = "Upload job failed without error message"

        if validationJob is None:
            # No validation job, so don't need to check it
            sess.commit()
            return responseStatus

        if responseStatus == "finished":
            # Check status of validation job if present
            responseStatus = FileHandler.VALIDATION_STATUS_MAP[validationStatus]
            if responseStatus == "finished" and errorsPresent:
                # If validation completed with errors, mark as failed
                responseStatus = "failed"
                uploadJob.error_message = "Validation completed but row-level errors were found"

        if responseStatus == "failed":
            if uploadJob.error_message is None and validationJob.error_message is None:
                if validationStatus == "invalid":
                    uploadJob.error_message = "Generated file had file-level errors"
                else:
                    uploadJob.error_message = "Validation job had an internal error"

            elif uploadJob.error_message is None:
                uploadJob.error_message = validationJob.error_message
        sess.commit()
        return responseStatus

    def getProtectedFiles(self):
        """ Returns a set of urls to protected files on the help page """
        response = {}
        if self.isLocal:
            response["urls"] = {}
            return JsonResponse.create(StatusCode.CLIENT_ERROR, response)

        response["urls"] = self.s3manager.getFileUrls(bucket_name=CONFIG_BROKER["static_files_bucket"], path=CONFIG_BROKER["help_files_path"])
        return JsonResponse.create(StatusCode.OK, response)

    def complete_generation(self, generation_id, file_type=None):
        """ For files D1 and D2, the API uses this route as a callback to load the generated file.
        Requires an 'href' key in the request that specifies the URL of the file to be downloaded

        Args:
            generationId - Unique key stored in file_generation_task table, used in callback to identify which submission
            this file is for.
            file_type - the type of file to be generated, D1 or D2. Only used when calling complete_generation for local development

        """
        sess = GlobalDB.db().session
        try:
            if generation_id is None:
                raise ResponseException(
                    "Must include a generation ID", StatusCode.CLIENT_ERROR)

            if not self.isLocal:
                # Pull url from request
                request_dict = RequestDictionary.derive(self.request)
                logger.debug('Request content => %s', request_dict)

                if 'href' not in request_dict:
                    raise ResponseException(
                        "Request must include href key with URL of D file",
                        StatusCode.CLIENT_ERROR
                    )

                url = request_dict['href']
                logger.debug('Download URL => %s', url)
            else:
                if file_type == "D1":
                    url = CONFIG_SERVICES["d1_file_path"]
                else:
                    url = CONFIG_SERVICES["d2_file_path"]

            #Pull information based on task key
            logger.debug('Pulling information based on task key...')
            task = sess.query(FileGenerationTask).filter(FileGenerationTask.generation_task_key == generation_id).one()
            job = sess.query(Job).filter_by(job_id = task.job_id).one()
            logger.debug('Loading D file...')
            result = self.load_d_file(url,job.filename,job.original_filename,job.job_id,self.isLocal)
            logger.debug('Load D file result => %s', result)
            return JsonResponse.create(StatusCode.OK,{"message":"File loaded successfully"})
        except ResponseException as e:
            return JsonResponse.error(e, e.status)
        except NoResultFound as e:
            # Did not find file generation task
            return JsonResponse.error(ResponseException("Generation task key not found", StatusCode.CLIENT_ERROR), StatusCode.CLIENT_ERROR)

    def list_submissions(self, page, limit, certified):
        """ List submission based on current page and amount to display. If provided, filter based on
        certification status """
        sess = GlobalDB.db().session

        offset = limit*(page-1)

        cgac_codes = [aff.cgac.cgac_code for aff in g.user.affiliations]
        query = sess.query(Submission).\
            filter(sa.or_(Submission.cgac_code.in_(cgac_codes),
                          Submission.user_id == g.user.user_id))
        if certified != 'mixed':
            query = query.filter_by(publishable=certified)
        submissions = query.order_by(Submission.updated_at.desc()).limit(limit).offset(offset).all()
        submission_details = []

        for submission in submissions:
            total_size = sess.query(func.sum(Job.file_size)).\
                filter_by(submission_id=submission.submission_id).\
                scalar() or 0

            status = get_submission_status(submission)
            if submission.user_id is None:
                submission_user_name = "No user"
            else:
                submission_user_name = sess.query(User).filter_by(user_id=submission.user_id).one().name
            submission_details.append({"submission_id": submission.submission_id,
                                       "last_modified": submission.updated_at.strftime('%Y-%m-%d'),
                                       "size": total_size, "status": status, "errors": submission.number_of_errors,
                                       "reporting_start_date": str(submission.reporting_start_date),
                                       "reporting_end_date": str(submission.reporting_end_date),
                                       "user": {"user_id": submission.user_id,
                                                "name": submission_user_name}})

        total_submissions = query.from_self().count()

        return JsonResponse.create(StatusCode.OK, {"submissions": submission_details, "total": total_submissions})

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

        timestamped_name = s3UrlHandler.getTimestampedFilename(
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


def get_status(submission):
    """ Get description and status of all jobs in the submission specified in request object

    Returns:
        A flask response object to be sent back to client, holds a JSON where each job ID has a dictionary holding file_type, job_type, status, and filename
    """
    try:
        sess = GlobalDB.db().session

        # Get jobs in this submission
        jobs = sess.query(Job).filter_by(submission_id=submission.submission_id)

        # Build dictionary of submission info with info about each job
        submission_info = {}
        submission_info["jobs"] = []
        submission_info["cgac_code"] = submission.cgac_code
        submission_info["created_on"] = submission.datetime_utc.strftime('%m/%d/%Y')
        # Include number of errors in submission
        submission_info["number_of_errors"] = submission.number_of_errors
        submission_info["number_of_rows"] = sess.query(
            func.sum(Job.number_of_rows)).\
            filter_by(submission_id=submission.submission_id).\
            scalar() or 0
        submission_info["last_updated"] = submission.updated_at.strftime("%Y-%m-%dT%H:%M:%S")
        # Format submission reporting date
        if submission.is_quarter_format:
            reporting_date = 'Q{}/{}'.format(
                int(submission.reporting_fiscal_period / 3), submission.reporting_fiscal_year)
        else:
            reporting_date = submission.reporting_start_date.strftime("%m/%Y")
        # Broker allows submission for a single quarter or a single month,
        # so reporting_period start and end dates reported by check_status
        # are always equal
        submission_info["reporting_period_start_date"] = submission_info["reporting_period_end_date"] = reporting_date

        for job in jobs:
            job_info = {}
            job_type = job.job_type.name

            if job_type != "csv_record_validation" and job_type != "validation":
                continue

            job_info["job_id"] = job.job_id
            job_info["job_status"] = job.job_status.name
            job_info["job_type"] = job_type
            job_info["filename"] = job.original_filename
            job_info["file_size"] = job.file_size
            job_info["number_of_rows"] = job.number_of_rows
            if job.file_type:
                job_info["file_type"] = job.file_type.name
            else:
                job_info["file_type"] = ''

            try:
                file_results = sess.query(File).options(joinedload("file_status")).filter(File.job_id == job.job_id).one()
                job_info["file_status"] = file_results.file_status.name
            except NoResultFound:
                # Job ID not in error database, probably did not make it to validation, or has not yet been validated
                job_info["file_status"] = ""
                job_info["missing_headers"] = []
                job_info["duplicated_headers"] = []
                job_info["error_type"] = ""
                job_info["error_data"] = []
                job_info["warning_data"] = []
            else:
                # If job ID was found in file, we should be able to get header error lists and file data
                # Get string of missing headers and parse as a list
                missing_header_string = file_results.headers_missing
                if missing_header_string is not None:
                    # Split header string into list, excluding empty strings
                    job_info["missing_headers"] = [n.strip() for n in missing_header_string.split(",") if len(n) > 0]
                else:
                    job_info["missing_headers"] = []
                # Get string of duplicated headers and parse as a list
                duplicated_header_string = file_results.headers_duplicated
                if duplicated_header_string is not None:
                    # Split header string into list, excluding empty strings
                    job_info["duplicated_headers"] = [n.strip() for n in duplicated_header_string.split(",") if len(n) > 0]
                else:
                    job_info["duplicated_headers"] = []
                job_info["error_type"] = getErrorType(job.job_id)
                job_info["error_data"] = getErrorMetricsByJobId(
                    job.job_id, job_type=='validation', severity_id=RULE_SEVERITY_DICT['fatal'])
                job_info["warning_data"] = getErrorMetricsByJobId(
                    job.job_id, job_type=='validation', severity_id=RULE_SEVERITY_DICT['warning'])

            submission_info["jobs"].append(job_info)

        # Build response object holding dictionary
        return JsonResponse.create(StatusCode.OK,submission_info)
    except ResponseException as e:
        return JsonResponse.error(e,e.status)
    except Exception as e:
        # Unexpected exception, this is a 500 server error
        return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)


def get_error_metrics(submission):
    """Returns an Http response object containing error information for every
    validation job in specified submission """
    sess = GlobalDB.db().session
    return_dict = {}
    try:
        jobs = sess.query(Job).filter_by(submission_id=submission.submission_id)
        for job in jobs :
            if job.job_type.name == 'csv_record_validation':
                file_type = job.file_type.name
                data_list = getErrorMetricsByJobId(job.job_id)
                return_dict[file_type]  = data_list
        return JsonResponse.create(StatusCode.OK,return_dict)
    except ( ValueError , TypeError ) as e:
        return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
    except ResponseException as e:
        return JsonResponse.error(e,e.status)
    except Exception as e:
        # Unexpected exception, this is a 500 server error
        return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)
