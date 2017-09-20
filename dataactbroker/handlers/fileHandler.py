import os
import smart_open
from collections import namedtuple
from datetime import datetime
import logging
from dateutil.relativedelta import relativedelta
from shutil import copyfile
import threading
import re

import calendar

import requests
from flask import g, request
import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy.orm import aliased
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import case
from werkzeug.utils import secure_filename

from dataactbroker.permissions import current_user_can, current_user_can_on_submission
from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import (
    CGAC, FREC, CFDAProgram, SubTierAgency, Zips, States, CountyCode, CityCode, ZipCity, CountryCode)
from dataactcore.models.errorModels import File
from dataactcore.models.stagingModels import (DetachedAwardFinancialAssistance, PublishedAwardFinancialAssistance,
                                              FPDSContractingOffice)
from dataactcore.models.jobModels import (Job, Submission, SubmissionNarrative, SubmissionSubTierAffiliation,
                                          RevalidationThreshold, CertifyHistory, CertifiedFilesHistory)
from dataactcore.models.userModel import User
from dataactcore.models.lookups import (
    FILE_TYPE_DICT, FILE_TYPE_DICT_LETTER, FILE_TYPE_DICT_LETTER_ID, PUBLISH_STATUS_DICT, JOB_STATUS_DICT,
    JOB_TYPE_DICT, RULE_SEVERITY_DICT, FILE_TYPE_DICT_ID, JOB_STATUS_DICT_ID, PUBLISH_STATUS_DICT_ID,
    FILE_TYPE_DICT_LETTER_NAME)
from dataactcore.models.views import SubmissionUpdatedView
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.report import get_cross_file_pairs, report_file_name
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.stringCleaner import StringCleaner
from dataactcore.interfaces.function_bag import (
    create_jobs, create_submission, get_error_metrics_by_job_jd, get_error_type, get_submission_status,
    mark_job_status, run_job_checks, get_last_validated_date, get_lastest_certified_date, get_fabs_meta)
from dataactbroker.handlers.fileGenerationHandler import generate_d_file, generate_e_file, generate_f_file

logger = logging.getLogger(__name__)


class FileHandler:
    """ Responsible for all tasks relating to file upload

    Static fields:
    FILE_TYPES -- list of file labels that can be included

    Instance fields:
    request -- A flask request object, comes with the request
    s3manager -- instance of S3Handler, manages calls to S3
    """

    # 1024 sounds like a good chunk size, we can change if needed
    CHUNK_SIZE = 1024
    FILE_TYPES = ["appropriations", "award_financial", "program_activity"]
    EXTERNAL_FILE_TYPES = ["award", "award_procurement", "executive_compensation", "sub_award"]
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
        self.s3manager = S3Handler()

    def get_error_report_urls_for_submission(self, submission_id, is_warning=False):
        """
        Gets the Signed URLs for download based on the submissionId
        """
        sess = GlobalDB.db().session
        try:
            self.s3manager = S3Handler()
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
                "frec_code": "frec_code",
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
            cant_create = not current_user_can('writer', submission.cgac_code, submission.frec_code)
            if cant_edit or cant_create:
                raise ResponseException(
                    "User does not have permission to create/modify that "
                    "submission", StatusCode.PERMISSION_DENIED
                )
            else:
                sess.add(submission)
                sess.commit()

            # build fileNameMap to be used in creating jobs
            self.build_file_map(request_params, FileHandler.FILE_TYPES, response_dict, upload_files, submission,
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
                            submission.submission_id,
                            S3Handler.get_timestamped_filename(filename)
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

        # change end_date date to the final date
        end_date = datetime.strptime(
                        str(end_date.year) + '/' +
                        str(end_date.month) + '/' +
                        str(calendar.monthrange(end_date.year, end_date.month)[1]),
                        '%Y/%m/%d'
                    ).date()

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
                # Populate start and end dates, these should be provided in MM/DD/YYYY format, using calendar year
                # (not fiscal year)
                request_dict = RequestDictionary(self.request)
                start, end = request_dict.get_value("start"), request_dict.get_value("end")
                if not (StringCleaner.is_date(start) and StringCleaner.is_date(end)):
                    raise ResponseException("Start or end date cannot be parsed into a date", StatusCode.CLIENT_ERROR)

            elif file_type not in ["E", "F"]:
                raise ResponseException("File type must be either D1, D2, E or F", StatusCode.CLIENT_ERROR)

        except ResponseException as e:
            return False, JsonResponse.error(e, e.status, file_type=file_type, status='failed')

        submission = sess.query(Submission).filter_by(submission_id=job.submission_id).one()

        # Generate and upload file to S3
        job = self.add_generation_job_info(file_type_name=file_type_name, job=job)
        upload_file_name, timestamped_name = job.filename, job.original_filename

        if file_type in ['D1', 'D2']:
            logger.debug('Adding job info for job id of %s', job.job_id)
            date_error = self.add_job_info_for_d_file(upload_file_name, timestamped_name, submission.submission_id,
                                                      file_type, file_type_name, start, end, job)
            if date_error is not None:
                return False, date_error

            agency_code = submission.frec_code if submission.frec_code else submission.cgac_code
            t = threading.Thread(target=generate_d_file, args=(file_type, agency_code, start, end, job.job_id,
                                                               timestamped_name, upload_file_name, self.isLocal))
        else:
            t = threading.Thread(
                target=generate_e_file if file_type == 'E' else generate_f_file,
                args=(submission.submission_id, job.job_id, timestamped_name, upload_file_name, self.isLocal))
        t.start()

        return True, None

    def add_job_info_for_d_file(self, upload_file_name, timestamped_name, submission_id, file_type, file_type_name,
                                start_date, end_date, job):
        """ Populates upload and validation job objects with start and end dates, filenames, and status

        Args:
            upload_file_name - Filename to use on S3
            timestamped_name - Version of filename without user ID
            submission_id - Submission to add D files to
            file_type - File type as either "D1" or "D2"
            file_type_name - Full name of file type
            start_date - Beginning of period for D file
            end_date - End of period for D file
            job - Job object for upload job
        """
        sess = GlobalDB.db().session
        val_job = sess.query(Job).filter(Job.submission_id == submission_id,
                                         Job.file_type_id == FILE_TYPE_DICT[file_type_name],
                                         Job.job_type_id == JOB_TYPE_DICT['csv_record_validation']).one()
        try:
            val_job.filename = upload_file_name
            val_job.original_filename = timestamped_name
            val_job.job_status_id = JOB_STATUS_DICT["waiting"]
            job.start_date = datetime.strptime(start_date, "%m/%d/%Y").date()
            job.end_date = datetime.strptime(end_date, "%m/%d/%Y").date()
            val_job.start_date = datetime.strptime(start_date, "%m/%d/%Y").date()
            val_job.end_date = datetime.strptime(end_date, "%m/%d/%Y").date()

            # Clear out error messages to prevent stale messages
            job.error_message = None
            val_job.error_message = None
        except ValueError as e:
            # Date was not in expected format
            exc = ResponseException(str(e), StatusCode.CLIENT_ERROR, ValueError)
            return JsonResponse.error(exc, exc.status, url="", start="", end="", file_type=file_type)

        return None

    def download_file(self, local_file_path, file_url, upload_name, response):
        """ Download a file locally from the specified URL, returns True if successful """
        if not self.isLocal:
            conn = self.s3manager.create_file_path(upload_name)
            with smart_open.smart_open(conn, 'w') as writer:
                # get request if it doesn't already exist
                if not response:
                    response = requests.get(file_url, stream=True)
                    # we only need to run this check if we haven't already
                    if response.status_code != 200:
                        # Could not download the file, return False
                        return False
                # write (stream) to file
                response.encoding = "utf-8"
                for chunk in response.iter_content(chunk_size=FileHandler.CHUNK_SIZE):
                    if chunk:
                        writer.write(chunk)
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

    def generate_file(self, submission_id, file_type):
        """ Start a file generation job for the specified file type """
        logger.debug('Starting D file generation')
        logger.debug('Submission ID = %s / File type = %s', submission_id, file_type)

        sess = GlobalDB.db().session

        # Check permission to submission
        error = submission_error(submission_id, file_type)
        if error:
            return error

        job = sess.query(Job).filter(Job.submission_id == submission_id,
                                     Job.file_type_id == FILE_TYPE_DICT_LETTER_ID[file_type],
                                     Job.job_type_id == JOB_TYPE_DICT['file_upload']).one()
        try:
            # Check prerequisites on upload job
            if not run_job_checks(job.job_id):
                raise ResponseException(
                    "Must wait for completion of prerequisite validation job",
                    StatusCode.CLIENT_ERROR)
        except ResponseException as exc:
            return JsonResponse.error(exc, exc.status)

        success, error_response = self.start_generation_job(job)

        logger.debug('Finished start_generation_job method')
        if not success:
            # If not successful, set job status as "failed"
            mark_job_status(job.job_id, "failed")
            return error_response

        submission = sess.query(Submission).filter_by(submission_id=submission_id).one()
        if file_type in ['D1', 'D2']:
            # Change the publish status back to updated if certified
            if submission.publish_status_id == PUBLISH_STATUS_DICT['published']:
                submission.publishable = False
                submission.publish_status_id = PUBLISH_STATUS_DICT['updated']
                sess.commit()

            # Set cross-file validation status to waiting if it's not already
            cross_file_job = sess.query(Job).filter(Job.submission_id == submission_id,
                                                    Job.job_type_id == JOB_TYPE_DICT['validation'],
                                                    Job.job_status_id != JOB_STATUS_DICT['waiting']).one_or_none()

            # No need to update it for each type of D file generation job, just do it once
            if cross_file_job:
                cross_file_job.job_status_id = JOB_STATUS_DICT['waiting']
                sess.commit()

        # Return same response as check generation route
        return self.check_generation(submission, file_type)

    def generate_detached_file(self, file_type, cgac_code, frec_code, start, end):
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

        # thread detached D file generation
        agency_code = frec_code if frec_code else cgac_code
        t = threading.Thread(target=generate_d_file, args=(file_type, agency_code, start, end, new_job.job_id,
                                                           new_job.original_filename, new_job.filename, self.isLocal))
        t.start()

        # Return same response as check generation route
        return self.check_detached_generation(new_job.job_id)

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

            job_data = {}
            # Check for existing submission
            existing_submission_id = request_params.get('existing_submission_id')
            if existing_submission_id:
                existing_submission = True
                existing_submission_obj = sess.query(Submission).\
                    filter_by(submission_id=existing_submission_id).\
                    one()
                jobs = sess.query(Job).filter(Job.submission_id == existing_submission_id)
                # set all jobs to their initial status of "waiting"
                jobs[0].job_status_id = JOB_STATUS_DICT['waiting']
                sess.commit()

            else:
                existing_submission = None
                existing_submission_obj = None

            if existing_submission_obj is not None:
                cgac_code = existing_submission_obj.cgac_code
                frec_code = existing_submission_obj.frec_code
            else:
                sub_tier_agency = sess.query(SubTierAgency).\
                    filter_by(sub_tier_agency_code=request_params["agency_code"]).one()
                cgac_code = None if sub_tier_agency.is_frec else sub_tier_agency.cgac.cgac_code
                frec_code = sub_tier_agency.frec.frec_code if sub_tier_agency.is_frec else None

            # get the cgac code associated with this sub tier agency
            job_data["cgac_code"] = cgac_code
            job_data["frec_code"] = frec_code
            job_data["d2_submission"] = True
            job_data['reporting_start_date'] = None
            job_data['reporting_end_date'] = None

            """
            Below lines commented out to temporarily allow all users
            to upload FABS data for all agencies during testing
            """
            # if not current_user_can('writer', job_data["cgac_code"]):
            #     raise ResponseException("User does not have permission to create jobs for this agency",
            #                             StatusCode.PERMISSION_DENIED)

            submission = create_submission(g.user.user_id, job_data, existing_submission_obj)
            sess.add(submission)
            sess.commit()
            if existing_submission_obj is None:
                sub_tier_agency_id = sub_tier_agency.sub_tier_agency_id
                sub_tier_affiliation = SubmissionSubTierAffiliation(submission_id=submission.submission_id,
                                                                    sub_tier_agency_id=sub_tier_agency_id)
                sess.add(sub_tier_affiliation)
                sess.commit()

            # build fileNameMap to be used in creating jobs
            self.build_file_map(request_params, ['detached_award'], response_dict, upload_files, submission)

            self.create_response_dict_for_submission(upload_files, submission, existing_submission,
                                                     response_dict, create_credentials)
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

        # We want to use first() here so we can see if the job is None so we can mark the status as invalid to
        # indicate that a status request is invoked for a job that isn't created yet
        upload_job = sess.query(Job).filter_by(job_id=job_id).one_or_none()
        response_dict = {'job_id': job_id, 'status': '', 'file_type': '', 'message': '', 'url': '', 'start': '',
                         'end': ''}
        if upload_job is None or upload_job.filename is None:
            response_dict['status'] = 'invalid'
            response_dict['message'] = 'No generation job found with the specified ID' if upload_job is None else\
                                       'No file has been generated for this submission.'
            return JsonResponse.create(StatusCode.OK, response_dict)

        file_type = FILE_TYPE_DICT_LETTER[upload_job.file_type_id]
        response_dict["status"] = JOB_STATUS_DICT_ID[upload_job.job_status_id]
        response_dict["file_type"] = file_type
        response_dict["message"] = upload_job.error_message or ""
        if response_dict["status"] is not 'finished':
            response_dict["url"] = "#"
        elif CONFIG_BROKER["use_aws"]:
            path, file_name = upload_job.filename.split("/")
            response_dict["url"] = S3Handler().get_signed_url(path=path, file_name=file_name, bucket_route=None,
                                                              method="GET")
        else:
            response_dict["url"] = upload_job.filename

        response_dict["start"] = upload_job.start_date.strftime("%m/%d/%Y") if upload_job.start_date is not None else ""
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

        upload_job = sess.query(Job).filter(Job.submission_id == submission.submission_id,
                                            Job.file_type_id == FILE_TYPE_DICT_LETTER_ID[file_type],
                                            Job.job_type_id == JOB_TYPE_DICT['file_upload']).one()

        if file_type in ['D1', 'D2']:
            validation_job = sess.query(Job).filter(Job.submission_id == submission.submission_id,
                                                    Job.file_type_id == FILE_TYPE_DICT_LETTER_ID[file_type],
                                                    Job.job_type_id == JOB_TYPE_DICT['csv_record_validation']).one()
        else:
            validation_job = None

        response_dict = {
            'status': map_generate_status(upload_job, validation_job),
            'file_type': file_type,
            'size': upload_job.file_size,
            'message': upload_job.error_message or "",
            'url': '#'
        }
        if CONFIG_BROKER["use_aws"] and response_dict["status"] is 'finished' and upload_job.filename:
            path, file_name = upload_job.filename.split("/")
            response_dict["url"] = S3Handler().get_signed_url(path=path, file_name=file_name, bucket_route=None,
                                                              method="GET")
        elif response_dict["status"] is 'finished' and upload_job.filename:
            response_dict["url"] = upload_job.filename

        # Pull start and end from jobs table if D1 or D2
        if file_type in ["D1", "D2"]:
            response_dict['start'] = upload_job.start_date.strftime('%m/%d/%Y') if upload_job.start_date else ''
            response_dict['end'] = upload_job.end_date.strftime('%m/%d/%Y') if upload_job.end_date else ''

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
                # remove all keys in the row that are not in the intermediate table
                temp_obj = row.__dict__
                temp_obj.pop('detached_award_financial_assistance_id', None)
                temp_obj.pop('submission_id', None)
                temp_obj.pop('job_id', None)
                temp_obj.pop('row_number', None)
                temp_obj.pop('is_valid', None)
                temp_obj.pop('_sa_instance_state', None)

                temp_obj = fabs_derivations(temp_obj, sess)

                # if it's a correction or deletion row and an old row is active, update the old row to be inactive
                if row.correction_late_delete_ind is not None and row.correction_late_delete_ind.upper() in ['C', 'D']:
                    check_row = sess.query(PublishedAwardFinancialAssistance).\
                        filter_by(afa_generated_unique=row.afa_generated_unique, is_active=True).one_or_none()
                    if check_row:
                        # just creating this as a variable because flake thinks the row is too long
                        row_id = check_row.published_award_financial_assistance_id
                        sess.query(PublishedAwardFinancialAssistance).\
                            filter_by(published_award_financial_assistance_id=row_id).\
                            update({"is_active": False, "updated_at": row.modified_at}, synchronize_session=False)

                # for all rows, insert the new row (active/inactive should be handled by fabs_derivations)
                new_row = PublishedAwardFinancialAssistance(**temp_obj)
                sess.add(new_row)

            sess.commit()
        except Exception as e:
            # rollback the changes if there are any errors. We want to submit everything together
            sess.rollback()
            return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)

        sess.query(Submission).filter_by(submission_id=submission_id).\
            update({"publish_status_id": PUBLISH_STATUS_DICT['published'], "certifying_user_id": g.user.user_id},
                   synchronize_session=False)
        certify_history = CertifyHistory(created_at=datetime.utcnow(), user_id=g.user.user_id,
                                         submission_id=submission_id)
        sess.add(certify_history)
        sess.commit()
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

    def add_generation_job_info(self, file_type_name, job=None, dates=None):
        # if job is None, that means the info being added is for detached d file generation
        sess = GlobalDB.db().session

        if job is None:
            job = Job(job_type_id=JOB_TYPE_DICT['file_upload'], user_id=g.user.user_id,
                      file_type_id=FILE_TYPE_DICT[file_type_name], start_date=dates['start_date'],
                      end_date=dates['end_date'])
            sess.add(job)

        timestamped_name = S3Handler.get_timestamped_filename(
            CONFIG_BROKER["".join([str(file_type_name), "_file_name"])])
        if self.isLocal:
            upload_file_name = "".join([CONFIG_BROKER['broker_files'], timestamped_name])
        else:
            upload_file_name = "".join([str(job.submission_id), "/", timestamped_name])

        # This will update the reference so no need to return the job, just the upload and timestamped file names
        job.message = None
        job.filename = upload_file_name
        job.original_filename = timestamped_name
        job.job_status_id = JOB_STATUS_DICT["running"]
        sess.commit()

        return job

    def build_file_map(self, request_params, file_type_list, response_dict, upload_files, submission,
                       existing_submission=False):
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
                        submission.submission_id,
                        S3Handler.get_timestamped_filename(file_name)
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
            self.s3manager = S3Handler(CONFIG_BROKER["aws_bucket"])
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

    def move_certified_files(self, submission, certify_history, is_local):
        sess = GlobalDB.db().session
        # Putting this here for now, get the uploads list
        jobs = sess.query(Job).filter(Job.submission_id == submission.submission_id,
                                      Job.job_type_id == JOB_TYPE_DICT['file_upload'],
                                      Job.filename.isnot(None)).all()
        possible_warning_files = [FILE_TYPE_DICT["appropriations"], FILE_TYPE_DICT["program_activity"],
                                  FILE_TYPE_DICT["award_financial"]]
        original_bucket = CONFIG_BROKER['aws_bucket']
        new_bucket = CONFIG_BROKER['certified_bucket']

        # route is used in multiple places, might as well just make it out here
        identifying_code = submission.cgac_code if submission.cgac_code else submission.frec_code
        new_route = '{}/{}/{}/{}/'.format(identifying_code, submission.reporting_fiscal_year,
                                          submission.reporting_fiscal_period // 3,
                                          certify_history.certify_history_id)
        for job in jobs:
            # non-local instances create a new path, local instances just use the existing one
            if not is_local:
                old_path_sections = job.filename.split("/")
                new_path = new_route + old_path_sections[-1]
            else:
                new_path = job.filename

            # get the warning file name for this file
            warning_file = None
            if job.file_type_id in possible_warning_files:
                # warning file is in the new path for non-local instances and just in its normal place for local ones
                if not is_local:
                    warning_file_name = report_file_name(submission.submission_id, True, job.file_type.name)
                    warning_file = new_route + warning_file_name

                    # move warning file while we're here
                    self.s3manager.copy_file(original_bucket=original_bucket, new_bucket=new_bucket,
                                             original_path="errors/" + warning_file_name, new_path=warning_file)
                else:
                    warning_file = CONFIG_SERVICES['error_report_path'] + report_file_name(submission.submission_id,
                                                                                           True, job.file_type.name)

            # get the narrative relating to the file
            narrative = sess.query(SubmissionNarrative).\
                filter_by(submission_id=submission.submission_id, file_type_id=job.file_type_id).one_or_none()
            if narrative:
                narrative = narrative.narrative

            # create the certified_files_history for this file
            certified_file_history = CertifiedFilesHistory(certify_history_id=certify_history.certify_history_id,
                                                           submission_id=submission.submission_id,
                                                           filename=new_path,
                                                           file_type_id=job.file_type_id, narrative=narrative,
                                                           warning_filename=warning_file)
            sess.add(certified_file_history)

            # only actually move the files if it's not a local submission
            if not is_local:
                self.s3manager.copy_file(original_bucket=original_bucket, new_bucket=new_bucket,
                                         original_path=job.filename, new_path=new_path)

        cross_list = {"B": "A", "C": "B", "D1": "C", "D2": "C"}
        for key, value in cross_list.items():
            first_file = FILE_TYPE_DICT_LETTER_NAME[value]
            second_file = FILE_TYPE_DICT_LETTER_NAME[key]

            # create warning file path
            if not is_local:
                warning_file_name = report_file_name(submission.submission_id, True, first_file, second_file)
                warning_file = new_route + warning_file_name

                # move the file if we aren't local
                self.s3manager.copy_file(original_bucket=original_bucket, new_bucket=new_bucket,
                                         original_path="errors/" + warning_file_name, new_path=warning_file)
            else:
                warning_file = CONFIG_SERVICES['error_report_path'] + report_file_name(submission.submission_id, True,
                                                                                       first_file, second_file)

            # add certified history
            certified_file_history = CertifiedFilesHistory(certify_history_id=certify_history.certify_history_id,
                                                           submission_id=submission.submission_id,
                                                           filename=None,
                                                           file_type_id=None, narrative=None,
                                                           warning_filename=warning_file)
            sess.add(certified_file_history)
        sess.commit()


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
    if not (submission.reporting_start_date or submission.reporting_end_date):
        return None
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
    frec = sess.query(FREC).\
        filter_by(frec_code=submission.frec_code).one_or_none()
    if cgac:
        agency_name = cgac.agency_name
    elif frec:
        agency_name = frec.agency_name
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

    fabs_meta = get_fabs_meta(submission.submission_id)

    return {
        'cgac_code': submission.cgac_code,
        'frec_code': submission.frec_code,
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
        'publish_status': submission.publish_status.name,
        'quarterly_submission': submission.is_quarter_format,
        'fabs_meta': fabs_meta
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


def list_submissions(page, limit, certified, sort='modified', order='desc', d2_submission=False):
    """ List submission based on current page and amount to display. If provided, filter based on
    certification status """
    sess = GlobalDB.db().session

    submission_updated_view = SubmissionUpdatedView()

    offset = limit * (page - 1)

    certifying_user = aliased(User)

    submission_columns = [Submission.submission_id, Submission.cgac_code, Submission.frec_code, Submission.user_id,
                          Submission.publish_status_id, Submission.d2_submission, Submission.number_of_warnings,
                          Submission.number_of_errors, Submission.updated_at, Submission.reporting_start_date,
                          Submission.reporting_end_date, Submission.certifying_user_id]

    cgac_columns = [CGAC.cgac_code, CGAC.agency_name.label('cgac_agency_name')]
    frec_columns = [FREC.frec_code, FREC.agency_name.label('frec_agency_name')]
    user_columns = [User.user_id, User.name, certifying_user.user_id.label('certifying_user_id'),
                    certifying_user.name.label('certifying_user_name')]

    view_columns = [submission_updated_view.submission_id,
                    submission_updated_view.updated_at.label('updated_at')]

    columns_to_query = submission_columns + cgac_columns + frec_columns + user_columns + view_columns

    query = sess.query(*columns_to_query).\
        outerjoin(User, Submission.user_id == User.user_id). \
        outerjoin(certifying_user, Submission.certifying_user_id == certifying_user.user_id). \
        outerjoin(CGAC, Submission.cgac_code == CGAC.cgac_code).\
        outerjoin(FREC, Submission.frec_code == FREC.frec_code).\
        outerjoin(submission_updated_view.table, submission_updated_view.submission_id == Submission.submission_id).\
        filter(Submission.d2_submission.is_(d2_submission))
    if not g.user.website_admin:
        cgac_codes = [aff.cgac.cgac_code for aff in g.user.affiliations if aff.cgac]
        frec_codes = [aff.frec.frec_code for aff in g.user.affiliations if aff.frec]
        query = query.filter(sa.or_(Submission.cgac_code.in_(cgac_codes),
                                    Submission.frec_code.in_(frec_codes),
                                    Submission.user_id == g.user.user_id))
    if certified != 'mixed':
        if certified == 'true':
            query = query.filter(Submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished'])
        else:
            query = query.filter(Submission.publish_status_id == PUBLISH_STATUS_DICT['unpublished'])

    total_submissions = query.count()

    options = {
        'modified': {'model': submission_updated_view, 'col': 'updated_at'},
        'reporting': {'model': Submission, 'col': 'reporting_start_date'},
        'agency': {'model': CGAC, 'col': 'agency_name'},
        'submitted_by': {'model': User, 'col': 'name'}
    }

    if not options.get(sort):
        sort = 'modified'

    sort_order = getattr(options[sort]['model'], options[sort]['col'])

    if sort == "agency":
        sort_order = case([
            (FREC.agency_name.isnot(None), FREC.agency_name),
            (CGAC.agency_name.isnot(None), CGAC.agency_name)
        ])

    if order == 'desc':
        sort_order = sort_order.desc()

    query = query.order_by(sort_order)

    query = query.limit(limit).offset(offset)

    return JsonResponse.create(StatusCode.OK, {
        "submissions": [serialize_submission(submission) for submission in query],
        "total": total_submissions
    })


def list_certifications(submission):
    """ List all certifications for a single submission including the file history that goes with them """
    sess = GlobalDB.db().session

    certify_history = sess.query(CertifyHistory).filter_by(submission_id=submission.submission_id).\
        order_by(CertifyHistory.created_at.desc()).all()

    # get the details for each of the certifications
    certifications = []
    for history in certify_history:
        certifying_user = sess.query(User).filter_by(user_id=history.user_id).one()

        # get all certified_files_history for this certification
        file_history = sess.query(CertifiedFilesHistory).filter_by(certify_history_id=history.certify_history_id).all()
        certified_files = []
        for file in file_history:
            # if there's a filename, add it to the list
            if file.filename is not None:
                certified_files.append({
                    "certified_files_history_id": file.certified_files_history_id,
                    "filename": file.filename.split("/")[-1],
                    "is_warning": False,
                    "narrative": file.narrative
                })

            # if there's a warning file, add it to the list
            if file.warning_filename is not None:
                certified_files.append({
                    "certified_files_history_id": file.certified_files_history_id,
                    "filename": file.warning_filename.split("/")[-1],
                    "is_warning": True,
                    "narrative": None
                })

        # after adding all certified files to the history, add the entire history entry to the certifications list
        certifications.append({
            "certify_date": history.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "certifying_user": {"name": certifying_user.name, "user_id": history.user_id},
            "certified_files": certified_files
        })

    return JsonResponse.create(StatusCode.OK, {"submission_id": submission.submission_id,
                                               "certifications": certifications})


def file_history_url(submission, file_history_id, is_warning, is_local):
    """ Get the signed URL for the specified file history """
    sess = GlobalDB.db().session

    file_history = sess.query(CertifiedFilesHistory).filter_by(certified_files_history_id=file_history_id).one_or_none()

    if not file_history:
        return JsonResponse.error(ValueError("Invalid certified_files_history_id"), StatusCode.CLIENT_ERROR)

    if file_history.submission_id != submission.submission_id:
        return JsonResponse.error(ValueError("Requested certified_files_history_id does not "
                                             "match submission_id provided"), StatusCode.CLIENT_ERROR)

    if is_warning and not file_history.warning_filename:
        return JsonResponse.error(ValueError("History entry has no warning file"), StatusCode.CLIENT_ERROR)

    if not is_warning and not file_history.filename:
        return JsonResponse.error(ValueError("History entry has no related file"), StatusCode.CLIENT_ERROR)

    # locally, just return the filepath
    if is_local:
        if is_warning:
            url = file_history.warning_filename
        else:
            url = file_history.filename
    else:
        if is_warning:
            file_array = file_history.warning_filename.split("/")
        else:
            file_array = file_history.filename.split("/")

        filename = file_array.pop()
        file_path = '/'.join(x for x in file_array)
        url = S3Handler().get_signed_url(file_path, filename, bucket_route=CONFIG_BROKER['certified_bucket'],
                                         method="GET")

    return JsonResponse.create(StatusCode.OK, {"url": url})


def serialize_submission(submission):
    """Convert the provided submission into a dictionary in a schema the
    frontend expects"""
    status = get_submission_status(submission)
    certified_on = get_lastest_certified_date(submission)
    agency_name = submission.cgac_agency_name if submission.cgac_agency_name else submission.frec_agency_name

    return {
        "submission_id": submission.submission_id,
        "last_modified": str(submission.updated_at),
        "status": status,
        "agency": agency_name if agency_name else 'N/A',
        # @todo why are these a different format?
        "reporting_start_date": str(submission.reporting_start_date) if submission.reporting_start_date else None,
        "reporting_end_date": str(submission.reporting_end_date) if submission.reporting_end_date else None,
        "user": {"user_id": submission.user_id,
                 "name": submission.name if submission.name else "No User"},
        "certifying_user": submission.certifying_user_name if submission.certifying_user_name else "",
        'publish_status': PUBLISH_STATUS_DICT_ID[submission.publish_status_id],
        "certified_on": str(certified_on) if certified_on else ""
    }


def submission_report_url(submission, warning, file_type, cross_type):
    """ Gets the signed URL for the specified file """
    file_name = report_file_name(
        submission.submission_id, warning, file_type, cross_type)
    if CONFIG_BROKER['local']:
        url = os.path.join(CONFIG_BROKER['broker_files'], file_name)
    else:
        url = S3Handler().get_signed_url("errors", file_name, method="GET")
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


def map_generate_status(upload_job, validation_job=None):
    """ Maps job status to file generation statuses expected by frontend """
    sess = GlobalDB.db().session
    upload_status = upload_job.job_status.name
    if validation_job is None:
        errors_present = False
        validation_status = None
    else:
        validation_status = validation_job.job_status.name
        if validation_job.number_of_errors > 0:
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


def fabs_derivations(obj, sess):

    # deriving total_funding_amount
    federal_action_obligation = obj['federal_action_obligation'] or 0
    non_federal_funding_amount = obj['non_federal_funding_amount'] or 0
    obj['total_funding_amount'] = federal_action_obligation + non_federal_funding_amount

    # deriving cfda_title from program_title in cfda_program table
    cfda_title = sess.query(CFDAProgram).filter_by(program_number=obj['cfda_number']).one_or_none()
    if cfda_title:
        obj['cfda_title'] = cfda_title.program_title
    else:
        logger.error("CFDA title not found for CFDA number %s", obj['cfda_number'])
        obj['cfda_title'] = None

    if obj['awarding_sub_tier_agency_c']:
        # deriving awarding agency name and code
        awarding_sub_tier = sess.query(SubTierAgency).\
            filter_by(sub_tier_agency_code=obj['awarding_sub_tier_agency_c']).one()
        use_frec = awarding_sub_tier.is_frec
        awarding_agency = awarding_sub_tier.frec if use_frec else awarding_sub_tier.cgac
        obj['awarding_agency_code'] = awarding_agency.frec_code if use_frec else awarding_agency.cgac_code
        obj['awarding_agency_name'] = awarding_agency.agency_name
        obj['awarding_sub_tier_agency_n'] = awarding_sub_tier.sub_tier_agency_name
    else:
        obj['awarding_agency_code'] = None
        obj['awarding_agency_name'] = None
        obj['awarding_sub_tier_agency_n'] = None

    # deriving funding agency name
    if obj['funding_agency_code']:
        funding_agency_name = sess.query(CGAC).filter_by(cgac_code=obj['funding_agency_code']).one()
        if not funding_agency_name:
            funding_agency_name = sess.query(FREC).filter_by(frec_code=obj['funding_agency_code']).one()
        obj['funding_agency_name'] = funding_agency_name.agency_name
    else:
        obj['funding_agency_name'] = None

    # deriving funding sub tier agency name
    if obj['funding_sub_tier_agency_co']:
        funding_sub_tier_agency_name = sess.query(SubTierAgency).\
            filter_by(sub_tier_agency_code=obj['funding_sub_tier_agency_co']).one()
        obj['funding_sub_tier_agency_na'] = funding_sub_tier_agency_name.sub_tier_agency_name
    else:
        obj['funding_sub_tier_agency_na'] = None

    # deriving ppop state name (ppop code is required so we don't have to check that it exists, just upper it)
    ppop_code = obj['place_of_performance_code'].upper()
    if ppop_code == '00*****':
        ppop_state = States(state_code=None, state_name='Multi-state')
    elif ppop_code == '00FORGN':
        ppop_state = States(state_code=None, state_name=None)
    else:
        ppop_state = sess.query(States).filter_by(state_code=ppop_code[:2]).one()
    obj['place_of_perform_state_nam'] = ppop_state.state_name

    # deriving place of performance values from zip4
    if obj['place_of_performance_zip4a'] and obj['place_of_performance_zip4a'] != 'city-wide':
        zip_five = obj['place_of_performance_zip4a'][:5]
        zip_four = None

        # if zip4 is 9 digits, set the zip_four value to the last 4 digits
        if len(obj['place_of_performance_zip4a']) > 5:
            zip_four = obj['place_of_performance_zip4a'][-4:]

        # if there's a 9-digit zip code, use both parts to get data, otherwise just grab the first
        # instance of the zip5 we find
        if zip_four:
            zip_info = sess.query(Zips).\
                filter_by(zip5=zip_five, zip_last4=zip_four).first()
        else:
            zip_info = sess.query(Zips).\
                filter_by(zip5=zip_five).first()

        # deriving ppop congressional district
        if not obj['place_of_performance_congr']:
            obj['place_of_performance_congr'] = zip_info.congressional_district_no

        # deriving PrimaryPlaceOfPerformanceCountyName
        county_info = sess.query(CountyCode).\
            filter_by(county_number=zip_info.county_number, state_code=zip_info.state_abbreviation).first()
        obj['place_of_perform_county_na'] = county_info.county_name

        # deriving PrimaryPlaceOfPerformanceCityName
        city_info = sess.query(ZipCity).filter_by(zip_code=zip_five).one()
        obj['place_of_performance_city'] = city_info.city_name
    # if there is no ppop zip4, we need to try to derive county/city info from the ppop code
    else:
        # if ppop_code is in county format,
        if re.match('^[A-Z]{2}\*\*\d{3}$', ppop_code):
            # getting county name
            county_code = ppop_code[-3:]
            county_info = sess.query(CountyCode).\
                filter_by(county_number=county_code, state_code=ppop_state.state_code).first()
            obj['place_of_perform_county_na'] = county_info.county_name
            obj['place_of_performance_city'] = None
        # if ppop_code is in city format
        elif re.match('^[A-Z]{2}\d{5}$', ppop_code) and not re.match('^[A-Z]{2}0{5}$', ppop_code):
            # getting city and county name
            city_code = ppop_code[-5:]
            city_info = sess.query(CityCode).filter_by(city_code=city_code, state_code=ppop_state.state_code).first()
            obj['place_of_performance_city'] = city_info.feature_name
            obj['place_of_perform_county_na'] = city_info.county_name

    # deriving legal entity stuff where applicable (record type is 2 in this case)
    if obj['legal_entity_zip5']:
        # legal entity city data
        city_info = sess.query(ZipCity).filter_by(zip_code=obj['legal_entity_zip5']).one()
        obj['legal_entity_city_name'] = city_info.city_name

        zip_data = None
        # if we have a legal entity zip+4 provided
        if obj['legal_entity_zip_last4']:
            zip_data = sess.query(Zips).\
                filter_by(zip5=obj['legal_entity_zip5'], zip_last4=obj['legal_entity_zip_last4']).first()

        # if legal_entity_zip_last4 returned no results (invalid combination), grab the first entry for this zip5
        # for derivation purposes. This will exist because we wouldn't have gotten this far if it didn't,
        # invalid legal_entity_zip5 when present is an error
        if not zip_data:
            zip_data = sess.query(Zips).filter_by(zip5=obj['legal_entity_zip5']).first()

        obj['legal_entity_congressional'] = zip_data.congressional_district_no

        # legal entity city data
        county_info = sess.query(CountyCode). \
            filter_by(county_number=zip_data.county_number, state_code=zip_data.state_abbreviation).first()
        obj['legal_entity_county_code'] = county_info.county_number
        obj['legal_entity_county_name'] = county_info.county_name

        # legal entity state data
        state_info = sess.query(States).filter_by(state_code=zip_data.state_abbreviation).one()
        obj['legal_entity_state_code'] = state_info.state_code
        obj['legal_entity_state_name'] = state_info.state_name

    # deriving legal entity stuff that's based on record type of 1 (ppop code must be in the format XX**### for these)
    if obj['record_type'] == 1:
        obj['legal_entity_city_name'] = None

        # legal entity county data
        county_code = ppop_code[-3:]
        county_info = sess.query(CountyCode). \
            filter_by(county_number=county_code, state_code=ppop_state.state_code).first()
        obj['legal_entity_county_code'] = county_code
        obj['legal_entity_county_name'] = county_info.county_name

        # legal entity state data
        obj['legal_entity_state_code'] = ppop_state.state_code
        obj['legal_entity_state_name'] = ppop_state.state_name

        # legal entity cd data
        obj['legal_entity_congressional'] = obj['place_of_performance_congr']

    # deriving awarding_office_name based off funding_office_code
    if obj['awarding_office_code']:
        award_office = sess.query(FPDSContractingOffice). \
            filter_by(contracting_office_code=obj['awarding_office_code']).one_or_none()
        if award_office:
            obj['awarding_office_name'] = award_office.contracting_office_name
        else:
            obj['awarding_office_name'] = None

    # deriving funding_office_name based off funding_office_code
    if obj['funding_office_code']:
        funding_office = sess.query(FPDSContractingOffice). \
            filter_by(contracting_office_code=obj['funding_office_code']).one_or_none()
        if funding_office:
            obj['funding_office_name'] = funding_office.contracting_office_name
        else:
            obj['funding_office_name'] = None

    if obj['legal_entity_city_name'] and obj['legal_entity_state_code']:
        city_code = sess.query(CityCode). \
            filter(func.lower(CityCode.feature_name) == func.lower(obj['legal_entity_city_name'].strip()),
                   func.lower(CityCode.state_code) == func.lower(
                       obj['legal_entity_state_code'].strip())).one_or_none()
        if city_code:
            obj['legal_entity_city_code'] = city_code.city_code

    # deriving primary_place_of_performance_country_name from primary_place_of_performnce_code
    if obj['primary_place_of_performance_country_code']:
        country_data = sess.query(CountryCode). \
            filter_by(country_code=obj['primary_place_of_performance_country_code'].upper()).one_or_none()
        if country_data:
            obj['primary_place_of_performance_country_name'] = country_data.country_name
        else:
            obj['primary_place_of_performance_country_name'] = None

    # deriving legal_entity_country_name from legal_entity_country_code
    if obj['legal_entity_country_code']:
        country_data = sess.query(CountryCode). \
            filter_by(country_code=obj['legal_entity_country_code'].upper()).one_or_none()
        if country_data:
            obj['legal_entity_country_name'] = country_data.country_name
        else:
            obj['legal_entity_country_name'] = None

    # deriving primary_place_of_performance_county_code when record_type is 1
    if obj['record_type'] == 1:
        county_data = sess.query(CountyCode). \
            filter_by(county_number=obj['place_of_performance_code'][-3:],
                      state_code=obj['place_of_performance_code'][:2]).one_or_none()
        obj['primary_place_of_performance_county_code'] = county_data.county_number
        obj['primary_place_of_performance_county_name'] = county_data.county_name

    # deriving primary_place_of_performance_county_code from primary_place_of_performance_zip4a
    if obj['record_type'] == 2 and obj['place_of_performance_zip4a']:
        zip_five = obj['place_of_performance_zip4a'][:5]

        # if zip4 is 9 digits, set the zip_four value to the last 4 digits
        if len(obj['place_of_performance_zip4a']) > 5:
            zip_four = obj['place_of_performance_zip4a'][-4:]

        # if there's a 9-digit zip code, use both parts to get data, otherwise just grab the first
        # instance of the zip5 we find
        if zip_four:
            zip_info = sess.query(Zips). \
                filter_by(zip5=zip_five, zip_last4=zip_four).first()
        else:
            zip_info = sess.query(Zips). \
                filter_by(zip5=zip_five).first()
        obj['primary_place_of_performance_county_code'] = zip_info.county_number
        county_data = sess.query(CountyCode). \
            filter_by(county_number=zip_info.county_number,
                      state_code=zip_info.state_abbreviation).one_or_none()
        if county_data:
            obj['primary_place_of_performance_county_name'] = county_data.county_name
        else:
            obj['primary_place_of_performance_county_name'] = None

    if obj['correction_late_delete_ind'] and obj['correction_late_delete_ind'].upper() == 'D':
        obj['is_active'] = False
    else:
        obj['is_active'] = True

    obj['modified_at'] = datetime.utcnow()

    return obj
