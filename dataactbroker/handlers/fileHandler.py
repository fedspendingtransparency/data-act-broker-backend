import os
from csv import reader
from datetime import datetime
import logging
from uuid import uuid4
from shutil import copyfile

import requests
from flask import session as flaskSession
from flask import session, request
from requests.exceptions import Timeout
from sqlalchemy import func
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.exc import NoResultFound
from werkzeug import secure_filename

from dataactbroker.handlers.aws.session import LoginSession
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.errorModels import File
from dataactcore.models.jobModels import FileGenerationTask, JobDependency, Job, Submission
from dataactcore.models.userModel import User
from dataactcore.models.lookups import FILE_STATUS_DICT, RULE_SEVERITY_DICT, JOB_STATUS_DICT, FILE_TYPE_DICT
from dataactcore.utils.jobQueue import generate_e_file, generate_f_file
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.report import (get_report_path, get_cross_report_name,
                                      get_cross_warning_report_name, get_cross_file_pairs)
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.stringCleaner import StringCleaner
from dataactcore.interfaces.function_bag import (
    checkNumberOfErrorsByJobId, getErrorType, run_job_checks,
    createFileIfNeeded, getErrorMetricsByJobId, get_submission_stats,
    mark_job_status)
from dataactvalidator.filestreaming.csv_selection import write_csv


_debug_logger = logging.getLogger('deprecated.debug')
_smx_logger = logging.getLogger('deprecated.smx')


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

    def __init__(self,request,interfaces = None,isLocal= False,serverPath =""):
        """ Create the File Handler

        Arguments:
            request - HTTP request object for this route
            interfaces - InterfaceHolder object to databases
            isLocal - True if this is a local installation that will not use AWS or Smartronix
            serverPath - If isLocal is True, this is used as the path to local files
        """
        self.request = request
        if(interfaces != None):
            self.interfaces = interfaces
            self.jobManager = interfaces.jobDb
            self.fileTypeMap = self.interfaces.jobDb.createFileTypeMap()
        self.isLocal = isLocal
        self.serverPath = serverPath
        self.s3manager = s3UrlHandler()


    def addInterfaces(self,interfaces):
        """ Add connections to databases

        Args:
            interfaces: InterfaceHolder object to DBs
        """
        self.interfaces = interfaces
        self.jobManager = interfaces.jobDb
        self.fileTypeMap = self.interfaces.jobDb.createFileTypeMap()

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

    def get_signed_url_for_submission_file(self):
        """ Gets the signed URL for the specified file """
        try:
            sess = GlobalDB.db().session
            self.s3manager = s3UrlHandler()
            safe_dictionary = RequestDictionary(self.request)
            file_name = safe_dictionary.getValue("file") + ".csv"
            submission_id = safe_dictionary.getValue("submission")
            submission = sess.query(Submission).filter_by(submission_id = submission_id).one()
            # Check that user has access to submission
            # If they don't, throw an exception
            self.check_submission_permission(submission)

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

    # Submit set of files
    def submit(self,name,CreateCredentials):
        """ Builds S3 URLs for a set of files and adds all related jobs to job tracker database

        Flask request should include keys from FILE_TYPES class variable above

        Arguments:
            name -- User ID from the session handler
            CreateCredentials - If True, will create temporary credentials for S3 uploads

        Returns:
        Flask response returned will have key_url and key_id for each key in the request
        key_url is the S3 URL for uploading
        key_id is the job id to be passed to the finalize_submission route
        """
        try:
            sess = GlobalDB.db().session
            response_dict= {}

            file_name_map = []
            safe_dictionary = RequestDictionary(self.request)
            submission_id = self.jobManager.createSubmission(name, safe_dictionary)
            existing_submission = False
            if safe_dictionary.exists("existing_submission_id"):
                existing_submission = True
                # Check if user has permission to specified submission
                submission = sess.query(Submission).filter_by(submission_id = submission_id).one()
                self.check_submission_permission(submission)

            # Build fileNameMap to be used in creating jobs
            for file_type in FileHandler.FILE_TYPES :
                # If filetype not included in request, and this is an update to an existing submission, skip it
                if not safe_dictionary.exists(file_type):
                    if existing_submission:
                        continue
                    # This is a new submission, all files are required
                    raise ResponseException("Must include all files for new submission", StatusCode.CLIENT_ERROR)

                filename = safe_dictionary.getValue(file_type)
                if safe_dictionary.exists(file_type):
                    if not self.isLocal:
                        upload_name =  str(name)+"/"+s3UrlHandler.getTimestampedFilename(filename)
                    else:
                        upload_name = filename
                    response_dict[file_type+"_key"] = upload_name
                    file_name_map.append((file_type,upload_name,filename))

            if not file_name_map and existing_submission:
                raise ResponseException("Must include at least one file for an existing submission",
                                        StatusCode.CLIENT_ERROR)
            if not existing_submission:
                # Don't add external files to existing submission
                for ext_file_type in FileHandler.EXTERNAL_FILE_TYPES:
                    filename = CONFIG_BROKER["".join([ext_file_type,"_file_name"])]

                    if not self.isLocal:
                        upload_name = str(name) + "/" + s3UrlHandler.getTimestampedFilename(filename)
                    else:
                        upload_name = filename
                    response_dict[ext_file_type + "_key"] = upload_name
                    file_name_map.append((ext_file_type, upload_name, filename))

            file_job_dict = self.jobManager.createJobs(file_name_map,submission_id,existing_submission)
            for file_type in file_job_dict.keys():
                if not "submission_id" in file_type:
                    response_dict[file_type+"_id"] = file_job_dict[file_type]
            if CreateCredentials and not self.isLocal:
                self.s3manager = s3UrlHandler(CONFIG_BROKER["aws_bucket"])
                response_dict["credentials"] = self.s3manager.getTemporaryCredentials(name)
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
            # Call error route directly, status code depends on exception
            return JsonResponse.error(e,e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)
        except:
            return JsonResponse.error(Exception("Failed to catch exception"),StatusCode.INTERNAL_ERROR)

    def finalize(self, job_id=None):
        """ Set upload job in job tracker database to finished, allowing dependent jobs to be started

        Flask request should include key "upload_id", which holds the job_id for the file_upload job

        Returns:
        A flask response object, if successful just contains key "success" with value True, otherwise value is False
        """
        sess = GlobalDB.db().session
        response_dict = {}
        try:
            if job_id is None:
                input_dictionary = RequestDictionary(self.request)
                job_id = input_dictionary.getValue("upload_id")

            # Compare user ID with user who submitted job, if no match return 400
            job = sess.query(Job).filter_by(job_id = job_id).one()
            submission = self.jobManager.getSubmissionForJob(job)
            # Check that user's agency matches submission cgac_code or "SYS", or user id matches submission's user
            user_id = LoginSession.getName(session)
            user_cgac = sess.query(User).filter(User.user_id == user_id).one().cgac_code
            if submission.user_id != user_id and submission.cgac_code != user_cgac and user_cgac != "SYS":
                # This user cannot finalize this job
                raise ResponseException("Cannot finalize a job for a different agency", StatusCode.CLIENT_ERROR)
            # Change job status to finished
            if self.jobManager.checkUploadType(job_id):
                mark_job_status(job_id, 'finished')
                response_dict["success"] = True
                return JsonResponse.create(StatusCode.OK,response_dict)
            else:
                raise ResponseException("Wrong job type for finalize route",StatusCode.CLIENT_ERROR)

        except ( ValueError , TypeError ) as e:
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)

    def check_submission_by_id(self, submission_id, file_type):
        """ Check that submission exists and user has permission to it

        Args:
            submission_id:  ID of submission to check
            file_type: file type that has been requested

        Returns:
            Tuple of boolean indicating whether submission has passed checks, and http response if not

        """
        error = None
        sess = GlobalDB.db().session

        try:
            submission = sess.query(Submission).filter_by(submission_id = submission_id).one()
        except ResponseException as exc:
            if isinstance(exc.wrappedException, NoResultFound):
                # Submission does not exist, change to 400 in this case since
                # route call specified a bad ID
                exc.status = StatusCode.CLIENT_ERROR
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
                error = JsonResponse.error(exc, exc.status, **response_dict)
            else:
                raise exc
        try:
            self.check_submission_permission(submission)
        except ResponseException as exc:
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
            error = JsonResponse.error(exc, exc.status, **response_dict)

        if error:
            return False, error
        return True, None

    def check_submission_permission(self,submission):
        """ Check if current user has permisson to access submission and return user object.

        Args:
            submission - Submission model object
        """
        sess = GlobalDB.db().session
        user = sess.query(User).filter(User.user_id == LoginSession.getName(session)).one()
        # Check that user has permission to see this submission, user must be within the agency of the submission, or be
        # the original user, or be in the 'SYS' agency
        submission_cgac = StringCleaner.cleanString(submission.cgac_code)
        user_cgac = StringCleaner.cleanString(user.cgac_code)
        if(submission_cgac != user_cgac and submission.user_id != user.user_id
           and user_cgac != "sys"):
            raise ResponseException("User does not have permission to view that submission",
                StatusCode.PERMISSION_DENIED)
        return user

    def get_status(self):
        """ Get description and status of all jobs in the submission specified in request object

        Returns:
            A flask response object to be sent back to client, holds a JSON where each job ID has a dictionary holding file_type, job_type, status, and filename
        """
        try:
            sess = GlobalDB.db().session
            input_dictionary = RequestDictionary(self.request)

            # Get submission
            submission_id = input_dictionary.getValue("submission_id")
            submission = sess.query(Submission).filter_by(submission_id = submission_id).one()

            # Check that user has access to submission
            self.check_submission_permission(submission)

            # Get jobs in this submission
            jobs = sess.query(Job).filter_by(submission_id=submission_id)

            # Build dictionary of submission info with info about each job
            submission_info = {}
            submission_info["jobs"] = []
            submission_info["cgac_code"] = submission.cgac_code
            submission_info["reporting_period_start_date"] = self.interfaces.jobDb.getStartDate(submission)
            submission_info["reporting_period_end_date"] = self.interfaces.jobDb.getEndDate(submission)
            submission_info["created_on"] = self.interfaces.jobDb.getFormattedDatetimeBySubmissionId(submission_id)
            # Include number of errors in submission
            submission_info["number_of_errors"] = submission.number_of_errors
            submission_info["number_of_rows"] = self.interfaces.jobDb.sumNumberOfRowsForJobList(jobs)
            submission_info["last_updated"] = submission.updated_at.strftime("%Y-%m-%dT%H:%M:%S")

            for job in jobs:
                job_info = {}
                job_type = job.job_type.name

                if job_type != "csv_record_validation" and job_type != "validation":
                    continue

                job_info["job_id"] = job.job_id
                job_info["job_status"] = job.job_status.name
                job_info["job_type"] = job_type
                job_info["filename"] = sess.query(Job).filter_by(job_id = job.job_id).one().original_filename
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
                # File size and number of rows not dependent on error DB
                # Get file size
                job_info["file_size"] = sess.query(Job).filter_by(job_id = job.job_id).one().file_size
                # Get number of rows in file
                job_info["number_of_rows"] = sess.query(Job).filter_by(job_id = job.job_id).one().number_of_rows

                try :
                    job_info["file_type"] = sess.query(Job).options(joinedload("file_type")).filter_by(job_id = job.job_id).one().file_type.name
                except:
                    # todo: add specific type of exception when we figure out what it is?
                    job_info["file_type"]  = ''
                submission_info["jobs"].append(job_info)

            # Build response object holding dictionary
            return JsonResponse.create(StatusCode.OK,submission_info)
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)

    def get_error_metrics(self) :
        """ Returns an Http response object containing error information for every validation job in specified submission """
        sess = GlobalDB.db().session
        return_dict = {}
        try:
            safe_dictionary = RequestDictionary(self.request)
            submission_id =  safe_dictionary.getValue("submission_id")

            # Check if user has permission to specified submission
            submission = sess.query(Submission).filter_by(submission_id=submission_id).one()
            self.check_submission_permission(submission)

            jobs = sess.query(Job).filter_by(submission_id=submission_id)
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

    def start_generation_job(self, submission_id, file_type):
        """ Initiates a file generation job

        Args:
            submission_id: ID of submission to start job for
            file_type: Type of file to be generated

        Returns:
            Tuple of boolean indicating successful start, and error response if False

        """
        jobDb = self.interfaces.jobDb
        sess = GlobalDB.db().session
        file_type_name = self.fileTypeMap[file_type]

        try:
            if file_type in ["D1", "D2"]:
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

        cgac_code = sess.query(Submission).filter_by(submission_id=submission_id).one().cgac_code

        # Generate and upload file to S3
        user_id = LoginSession.getName(session)
        timestamped_name = s3UrlHandler.getTimestampedFilename(CONFIG_BROKER["".join([str(file_type_name),"_file_name"])])
        if self.isLocal:
            upload_file_name = "".join([CONFIG_BROKER['broker_files'], timestamped_name])
        else:
            upload_file_name = "".join([str(user_id), "/", timestamped_name])

        job = jobDb.getJobBySubmissionFileTypeAndJobType(submission_id, file_type_name, "file_upload")
        job.filename = upload_file_name
        job.original_filename = timestamped_name
        job.job_status_id = JOB_STATUS_DICT["running"]
        jobDb.session.commit()
        if file_type in ["D1", "D2"]:
            _debug_logger.debug('Adding job info for job id of %s', job.job_id)
            return self.add_job_info_for_d_file(upload_file_name, timestamped_name, submission_id, file_type, file_type_name, start_date, end_date, cgac_code, job)
        elif file_type == 'E':
            generate_e_file.delay(
                submission_id, job.job_id, timestamped_name,
                upload_file_name, self.isLocal)
        elif file_type == 'F':
            generate_f_file.delay(
                submission_id, job.job_id, timestamped_name,
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
        jobDb = self.interfaces.jobDb
        try:
            val_job = jobDb.getJobBySubmissionFileTypeAndJobType(submission_id, file_type_name, "csv_record_validation")
            val_job.filename = upload_file_name
            val_job.original_filename = timestamped_name
            val_job.job_status_id = JOB_STATUS_DICT["waiting"]
            job.start_date = datetime.strptime(start_date,"%m/%d/%Y").date()
            job.end_date = datetime.strptime(end_date,"%m/%d/%Y").date()
            val_job.start_date = datetime.strptime(start_date,"%m/%d/%Y").date()
            val_job.end_date = datetime.strptime(end_date,"%m/%d/%Y").date()
            # Generate random uuid and store generation task
            task_key = uuid4()
            task = FileGenerationTask(generation_task_key = task_key, submission_id = submission_id, file_type_id = FILE_TYPE_DICT[file_type_name], job_id = job.job_id)
            jobDb.session.add(task)

            jobDb.session.commit()
        except ValueError as e:
            # Date was not in expected format
            exc = ResponseException(str(e),StatusCode.CLIENT_ERROR,ValueError)
            return False, JsonResponse.error(exc, exc.status, url = "", start = "", end = "",  file_type = file_type)

        if not self.isLocal:
            # Create file D API URL with dates and callback URL
            callback = "{}://{}:{}/v1/complete_generation/{}/".format(CONFIG_SERVICES["protocol"],CONFIG_SERVICES["broker_api_host"], CONFIG_SERVICES["broker_api_port"],task_key)
            _debug_logger.debug('Callback URL for %s: %s', file_type, callback)
            get_url = CONFIG_BROKER["".join([file_type_name, "_url"])].format(cgac_code, start_date, end_date, callback)

            _debug_logger.debug('Calling D file API => %s', get_url)
            try:
                if not self.call_d_file_api(get_url):
                    self.handleEmptyResponse(job, val_job)
            except Timeout as e:
                exc = ResponseException(str(e), StatusCode.CLIENT_ERROR, Timeout)
                return False, JsonResponse.error(e, exc.status, url="", start="", end="", file_type=file_type)
        else:
            self.completeGeneration(task.generation_task_key, file_type)

        return True, None

    def handleEmptyResponse(self, job, valJob):
        """ Handles an empty response from the D file API by marking jobs as finished with no errors or rows

        Args:
            job - Job object for upload job
            valJob - Job object for validation job
        """
        sess = GlobalDB.db().session
        jobDb = self.interfaces.jobDb
        # No results found, skip validation and mark as finished
        jobDb.session.query(JobDependency).filter(JobDependency.prerequisite_id == job.job_id).delete()
        jobDb.session.commit()
        mark_job_status(job.job_id,"finished")
        job.filename = None
        if valJob is not None:
            mark_job_status(valJob.job_id, "finished")
            # Create File object for this validation job
            valFile = createFileIfNeeded(valJob.job_id, filename = valJob.filename)
            valFile.file_status_id = FILE_STATUS_DICT['complete']
            sess.commit()
            valJob.number_of_rows = 0
            valJob.number_of_rows_valid = 0
            valJob.file_size = 0
            valJob.number_of_errors = 0
            valJob.number_of_warnings = 0
            valJob.filename = None
            jobDb.session.commit()

    def get_xml_response_content(self, api_url):
        """ Retrieve XML Response from the provided API url """
        _debug_logger.debug('Getting XML response')
        return requests.get(api_url, verify=False, timeout=120).text

    def call_d_file_api(self, api_url):
        """ Call D file API, return True if results found, False otherwise """
        # Check for numFound = 0
        return "numFound='0'" not in self.get_xml_response_content(api_url)

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
        else:
            try:
                copyfile(file_url, local_file_path)
            except FileNotFoundError:
                raise ResponseException('Source file ' + file_url + ' does not exist.', StatusCode.INTERNAL_ERROR)
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
        job_manager = self.interfaces.jobDb
        sess = GlobalDB.db().session
        try:
            full_file_path = "".join([CONFIG_BROKER['d_file_storage_path'], timestamped_name])

            _smx_logger.debug('Downloading file...')
            if not self.download_file(full_file_path, url):
                # Error occurred while downloading file, mark job as failed and record error message
                mark_job_status(job_id, "failed")
                job = sess.query(Job).filter_by(job_id = job_id).one()
                file_type = sess.query(Job).options(joinedload("file_type")).filter_by(job_id = job_id).one().file_type.name
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

            _smx_logger.debug('Marking job id of %s', job_id)
            mark_job_status(job_id, "finished")
            return {"message": "Success", "file_name": timestamped_name}
        except Exception as e:
            _smx_logger.exception('Exception caught => %s', e)
            # Log the error
            JsonResponse.error(e,500)
            sess.query(Job).filter_by(job_id=job_id).one().error_message = str(e)
            mark_job_status(job_id, "failed")
            sess.commit()
            raise e

    def getRequestParamsForGenerate(self):
        """ Pull information out of request object and return it

        Returns: tuple of submission ID and file type

        """
        requestDict = RequestDictionary(self.request)
        if not (requestDict.exists("submission_id") and requestDict.exists("file_type")):
            raise ResponseException("Generate file route requires submission_id and file_type",
                                    StatusCode.CLIENT_ERROR)

        submission_id = requestDict.getValue("submission_id")
        file_type = requestDict.getValue("file_type")
        return submission_id, file_type

    def generateFile(self):
        """ Start a file generation job for the specified file type """
        _debug_logger.debug('Starting D file generation')
        submission_id, file_type = self.getRequestParamsForGenerate()

        _debug_logger.debug('Submission ID = %s / File type = %s',
                            submission_id, file_type)
        # Check permission to submission
        success, error_response = self.check_submission_by_id(submission_id, file_type)
        if not success:
            return error_response

        job = self.interfaces.jobDb.getJobBySubmissionFileTypeAndJobType(submission_id, self.fileTypeMap[file_type], "file_upload")
        try:
            # Check prerequisites on upload job
            if not run_job_checks(job.job_id):
                raise ResponseException(
                    "Must wait for completion of prerequisite validation job",
                    StatusCode.CLIENT_ERROR
                )
        except ResponseException as exc:
            return JsonResponse.error(exc, exc.status)

        success, error_response = self.start_generation_job(submission_id,file_type)

        _debug_logger.debug('Finished start_generation_job method')
        if not success:
            # If not successful, set job status as "failed"
            self.interfaces.mark_job_status(job.job_id, "failed")
            return error_response

        # Return same response as check generation route
        return self.checkGeneration(submission_id, file_type)

    def checkGeneration(self, submission_id = None, file_type = None):
        """ Return information about file generation jobs

        Returns:
            Response object with keys status, file_type, url, message.  If file_type is D1 or D2, also includes start and end.
        """
        if submission_id is None or file_type is None:
            submission_id, file_type = self.getRequestParamsForGenerate()
        # Check permission to submission
        self.check_submission_by_id(submission_id, file_type)

        uploadJob = self.interfaces.jobDb.getJobBySubmissionFileTypeAndJobType(submission_id, self.fileTypeMap[file_type], "file_upload")
        if file_type in ["D1","D2"]:
            validationJob = self.interfaces.jobDb.getJobBySubmissionFileTypeAndJobType(submission_id, self.fileTypeMap[file_type], "csv_record_validation")
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

        return JsonResponse.create(StatusCode.OK,responseDict)

    def mapGenerateStatus(self, uploadJob, validationJob = None):
        """ Maps job status to file generation statuses expected by frontend """
        uploadStatus = self.interfaces.jobDb.getJobStatusNameById(uploadJob.job_status_id)
        if validationJob is None:
            errorsPresent = False
            validationStatus = None
        else:
            validationStatus = self.interfaces.jobDb.getJobStatusNameById(validationJob.job_status_id)
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
            self.interfaces.jobDb.session.commit()
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
        self.interfaces.jobDb.session.commit()
        return responseStatus

    def getProtectedFiles(self):
        """ Returns a set of urls to protected files on the help page """
        response = {}
        if self.isLocal:
            response["urls"] = {}
            return JsonResponse.create(StatusCode.CLIENT_ERROR, response)

        response["urls"] = self.s3manager.getFileUrls(bucket_name=CONFIG_BROKER["static_files_bucket"], path=CONFIG_BROKER["help_files_path"])
        return JsonResponse.create(StatusCode.OK, response)

    def completeGeneration(self, generationId, file_type=None):
        """ For files D1 and D2, the API uses this route as a callback to load the generated file.
        Requires an 'href' key in the request that specifies the URL of the file to be downloaded

        Args:
            generationId - Unique key stored in file_generation_task table, used in callback to identify which submission
            this file is for.
            file_type - the type of file to be generated, D1 or D2. Only used when calling completeGeneration for local development

        """
        sess = GlobalDB.db().session
        try:
            if generationId is None:
                raise ResponseException(
                    "Must include a generation ID", StatusCode.CLIENT_ERROR)

            if not self.isLocal:
                # Pull url from request
                request_dict = RequestDictionary.derive(self.request)
                _smx_logger.debug('Request content => %s', request_dict)

                if 'href' not in request_dict:
                    raise ResponseException(
                        "Request must include href key with URL of D file",
                        StatusCode.CLIENT_ERROR
                    )

                url = request_dict['href']
                _smx_logger.debug('Download URL => %s', url)
            else:
                if file_type == "D1":
                    url = CONFIG_SERVICES["d1_file_path"]
                else:
                    url = CONFIG_SERVICES["d2_file_path"]

            #Pull information based on task key
            _smx_logger.debug('Pulling information based on task key...')
            task = self.interfaces.jobDb.session.query(FileGenerationTask).options(joinedload(FileGenerationTask.file_type)).filter(FileGenerationTask.generation_task_key == generationId).one()
            job = sess.query(Job).filter_by(job_id = task.job_id).one()
            _smx_logger.debug('Loading D file...')
            result = self.load_d_file(url,job.filename,job.original_filename,job.job_id,self.isLocal)
            _smx_logger.debug('Load D file result => %s', result)
            return JsonResponse.create(StatusCode.OK,{"message":"File loaded successfully"})
        except ResponseException as e:
            return JsonResponse.error(e, e.status)
        except NoResultFound as e:
            # Did not find file generation task
            return JsonResponse.error(ResponseException("Generation task key not found", StatusCode.CLIENT_ERROR), StatusCode.CLIENT_ERROR)

    def getObligations(self):
        sess = GlobalDB.db().session
        input_dictionary = RequestDictionary(self.request)

        # Get submission
        submission_id = input_dictionary.getValue("submission_id")
        submission = sess.query(Submission).filter_by(submission_id=submission_id).one()

        # Check that user has access to submission
        self.check_submission_permission(submission)

        obligations_info = get_submission_stats(submission_id)

        return JsonResponse.create(StatusCode.OK,obligations_info)

    def list_submissions(self, page, limit, certified):
        """ List submission based on current page and amount to display. If provided, filter based on
        certification status """
        user_id = LoginSession.getName(flaskSession)
        sess = GlobalDB.db().session
        user = sess.query(User).filter(User.user_id == user_id).one()

        offset = limit*(page-1)

        query = sess.query(Submission).filter(Submission.cgac_code == user.cgac_code)
        if certified != 'mixed':
            query = query.filter_by(publishable=certified)
        submissions = query.order_by(Submission.updated_at.desc()).limit(limit).offset(offset).all()
        submission_details = []

        for submission in submissions:
            total_size = sess.query(func.sum(Job.file_size)).\
                filter_by(submission_id=submission.submission_id).\
                scalar() or 0

            status = self.interfaces.jobDb.getSubmissionStatus(submission)
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
