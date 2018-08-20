import boto3
import calendar
import logging
import os
import requests
import smart_open
import sqlalchemy as sa
import threading

from collections import namedtuple
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from flask import g, current_app
from shutil import copyfile
from sqlalchemy import func, and_, desc, or_
from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import case

from dataactbroker.handlers.fabsDerivationsHandler import fabs_derivations
from dataactbroker.handlers.submission_handler import (create_submission, get_submission_status, get_submission_files,
                                                       reporting_date, job_to_dict)
from dataactbroker.permissions import current_user_can_on_submission

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import (
    create_jobs, get_error_metrics_by_job_id, get_fabs_meta, mark_job_status, get_last_validated_date,
    get_lastest_certified_date)

from dataactcore.models.domainModels import CGAC, FREC, SubTierAgency, States, CountryCode, CFDAProgram, CountyCode
from dataactcore.models.jobModels import (Job, Submission, SubmissionNarrative, SubmissionSubTierAffiliation,
                                          RevalidationThreshold, CertifyHistory, CertifiedFilesHistory, FileRequest)
from dataactcore.models.lookups import (
    FILE_TYPE_DICT, FILE_TYPE_DICT_LETTER, FILE_TYPE_DICT_LETTER_ID, PUBLISH_STATUS_DICT, JOB_TYPE_DICT,
    FILE_TYPE_DICT_ID, JOB_STATUS_DICT, JOB_STATUS_DICT_ID, PUBLISH_STATUS_DICT_ID, FILE_TYPE_DICT_LETTER_NAME)
from dataactcore.models.stagingModels import (DetachedAwardFinancialAssistance, PublishedAwardFinancialAssistance,
                                              FPDSContractingOffice)
from dataactcore.models.userModel import User
from dataactcore.models.views import SubmissionUpdatedView

from dataactcore.utils import fileD2
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.report import report_file_name
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.stringCleaner import StringCleaner

from dataactvalidator.filestreaming.csv_selection import write_query_to_file
from dataactvalidator.validation_handlers.file_generation_handler import check_file_generation, start_generation_job

logger = logging.getLogger(__name__)


class FileHandler:
    """ Responsible for all tasks relating to file upload

        Attributes:
            request: A Flask object containing the route request
            is_local: A boolean flag indicating whether the application is being run locally or not
            server_path: A string containing the path to the server files (only applicable when run locally)
            s3manager: An instance of S3Handler that can be used for all interactions with S3

        Class Attributes:
            UploadFile: A tuple used to store details about the file being uploaded

        Constants:
            CHUNK_SIZE: An integer indicating the size of each chunk to read/stream
            FILE_TYPES: An array of the types of files users can upload to DABS (as strings)
            EXTERNAL_FILE_TYPES: An array of the types of files received from other sources in DABS (as strings)
            VALIDATOR_RESPONSE_FILE: A string indicating a file name
    """

    # 1024 sounds like a good chunk size, we can change if needed
    CHUNK_SIZE = 1024
    FILE_TYPES = ["appropriations", "award_financial", "program_activity"]
    EXTERNAL_FILE_TYPES = ["award", "award_procurement", "executive_compensation", "sub_award"]
    VALIDATOR_RESPONSE_FILE = "validatorResponse"

    UploadFile = namedtuple('UploadFile', ['file_type', 'upload_name', 'file_name', 'file_letter'])

    def __init__(self, route_request, is_local=False, server_path=""):
        """ Create the File Handler

            Args:
                route_request: HTTP request object for this route
                is_local: True if this is a local installation that will not use AWS or Smartronix
                server_path: If is_local is True, this is used as the path to local files
        """
        self.request = route_request
        self.is_local = is_local
        self.server_path = server_path
        self.s3manager = S3Handler()

    def validate_upload_dabs_files(self):
        """ Validate whether the files can be created (if a new submission is being created) or not (does the
            submission exist for this date range already)

            Returns:
                Results of submit function or a JsonResponse object containing a failure message
        """
        sess = GlobalDB.db().session
        submission_request = RequestDictionary.derive(self.request)

        start_date = submission_request.get('reporting_period_start_date')
        end_date = submission_request.get('reporting_period_end_date')
        is_quarter = str(submission_request.get('is_quarter')).upper() == 'TRUE'

        # If both start and end date are provided, make sure no other submission is already published for that period
        if not (start_date is None or end_date is None):
            formatted_start_date, formatted_end_date = FileHandler.check_submission_dates(start_date,
                                                                                          end_date, is_quarter)

            submissions = sess.query(Submission).filter(
                Submission.cgac_code == submission_request.get('cgac_code'),
                Submission.frec_code == submission_request.get('frec_code'),
                Submission.reporting_start_date == formatted_start_date,
                Submission.reporting_end_date == formatted_end_date,
                Submission.is_quarter_format == is_quarter,
                Submission.d2_submission.is_(False),
                Submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished'])

            if 'existing_submission_id' in submission_request:
                submissions.filter(Submission.submission_id !=
                                   submission_request['existing_submission_id'])

            submissions = submissions.order_by(desc(Submission.created_at))

            if submissions.count() > 0:
                data = {
                    "message": "A submission with the same period already exists.",
                    "submissionId": submissions[0].submission_id
                }
                return JsonResponse.create(StatusCode.CLIENT_ERROR, data)

        return self.submit(sess)

    @staticmethod
    def validate_submit_file_params(request_params):
        """ Makes sure that the request params for DABS submissions are valid for a file upload call.

            Args:
                request_params: the object containing the request params for the API call

            Raises:
                ResponseException: if not all required params are present in a new submission or none of the params
                    are present in a re-upload for an existing submission
        """
        existing_submission_id = request_params.get('existing_submission_id')
        param_count = 0
        for file_type in FileHandler.FILE_TYPES:
            if "_files" in request_params and request_params['_files'].get(file_type):
                param_count += 1

        if not existing_submission_id and param_count != len(FileHandler.FILE_TYPES):
            raise ResponseException("Must include all files for a new submission", StatusCode.CLIENT_ERROR)

        if existing_submission_id and param_count == 0:
            raise ResponseException("Must include at least one file for an existing submission",
                                    StatusCode.CLIENT_ERROR)

    def submit(self, sess):
        """ Builds S3 URLs for a set of files and adds all related jobs to job tracker database

            Flask request should include keys from FILE_TYPES class variable above

            Args:
                sess: current DB session

            Returns:
                JsonResponse object that contains the results of create_response_dict or the details of the failure
                Flask response returned will have key_url and key_id for each key in the request
                key_url is the S3 URL for uploading
                key_id is the job id to be passed to the finalize_submission route
        """
        json_response, submission = None, None
        try:
            upload_files = []
            request_params = RequestDictionary.derive(self.request)

            self.validate_submit_file_params(request_params)

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
                existing_submission_obj = sess.query(Submission).filter_by(submission_id=existing_submission_id).one()
                # If the existing submission is a FABS submission, stop everything
                if existing_submission_obj.d2_submission:
                    raise ResponseException("Existing submission must be a DABS submission", StatusCode.CLIENT_ERROR)
            else:
                existing_submission = None
                existing_submission_obj = None

            for request_field, submission_field in request_submission_mapping.items():
                if request_field in request_params:
                    request_value = request_params[request_field]
                    submission_data[submission_field] = request_value
                # all of those fields are required unless existing_submission_id is present
                elif 'existing_submission_id' not in request_params:
                    raise ResponseException('{} is required'.format(request_field), StatusCode.CLIENT_ERROR, ValueError)

            # make sure submission dates are valid
            formatted_start_date, formatted_end_date = FileHandler.check_submission_dates(
                submission_data.get('reporting_start_date'),
                submission_data.get('reporting_end_date'),
                str(submission_data.get('is_quarter_format')).upper() == 'TRUE',
                existing_submission_obj)
            submission_data['reporting_start_date'] = formatted_start_date
            submission_data['reporting_end_date'] = formatted_end_date

            submission = create_submission(g.user.user_id, submission_data, existing_submission_obj)
            sess.add(submission)
            sess.commit()

            # build fileNameMap to be used in creating jobs
            file_dict = request_params["_files"]
            self.build_file_map(file_dict, FileHandler.FILE_TYPES, upload_files, submission)

            if not existing_submission:
                # don't add external files to existing submission
                for ext_file_type in FileHandler.EXTERNAL_FILE_TYPES:
                    filename = CONFIG_BROKER["".join([ext_file_type, "_file_name"])]

                    if not self.is_local:
                        upload_name = "{}/{}".format(
                            submission.submission_id,
                            S3Handler.get_timestamped_filename(filename)
                        )
                    else:
                        upload_name = filename
                    upload_files.append(FileHandler.UploadFile(
                        file_type=ext_file_type,
                        upload_name=upload_name,
                        file_name=filename,
                        file_letter=FILE_TYPE_DICT_LETTER[FILE_TYPE_DICT[ext_file_type]]
                    ))

            # Add jobs or update existing ones
            job_dict = self.create_jobs_for_submission(upload_files, submission, existing_submission)

            def upload(file_ref, file_type, app, current_user):
                filename_key = [x.upload_name for x in upload_files if x.file_type == file_type][0]
                bucket_name = CONFIG_BROKER["broker_files"] if self.is_local else CONFIG_BROKER["aws_bucket"]
                if CONFIG_BROKER['use_aws']:
                    s3 = boto3.client('s3', region_name='us-gov-west-1')
                    s3.upload_fileobj(file_ref, bucket_name, filename_key)
                else:
                    file_ref.save(filename_key)
                with app.app_context():
                        g.user = current_user
                        self.finalize(job_dict[file_type + "_id"])
            for file_type, file_ref in request_params["_files"].items():
                t = threading.Thread(target=upload, args=(file_ref, file_type,
                                                          current_app._get_current_object(), g.user))
                t.start()
                t.join()
            api_response = {"success": "true", "submission_id": submission.submission_id}
            json_response = JsonResponse.create(StatusCode.OK, api_response)
        except (ValueError, TypeError, NotImplementedError) as e:
            json_response = JsonResponse.error(e, StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            # call error route directly, status code depends on exception
            json_response = JsonResponse.error(e, e.status)
        except Exception as e:
            # handle unexpected exception as a 500 server error
            json_response = JsonResponse.error(e, StatusCode.INTERNAL_ERROR)
        finally:
            # handle a missing JSON response
            if json_response is None:
                json_response = JsonResponse.error(Exception("Failed to catch exception"), StatusCode.INTERNAL_ERROR)

            # handle errors within upload jobs
            if json_response.status_code != StatusCode.OK and submission:
                jobs = sess.query(Job).filter(Job.submission_id == submission.submission_id,
                                              Job.job_type_id == JOB_TYPE_DICT['file_upload'],
                                              Job.job_status_id == JOB_STATUS_DICT['running'],
                                              Job.file_type_id.in_(FILE_TYPE_DICT_LETTER_ID['A'],
                                                                   FILE_TYPE_DICT_LETTER_ID['B'],
                                                                   FILE_TYPE_DICT_LETTER_ID['C'])).all()
                for job in jobs:
                    job.job_status_id = JOB_STATUS_DICT['failed']
                sess.commit()

            return json_response

    @staticmethod
    def check_submission_dates(start_date, end_date, is_quarter, existing_submission=None):
        """ Check validity of incoming submission start and end dates.

            Args:
                start_date: the start date of the submission in string format
                end_date: the end date of the submission in string format
                is_quarter: a boolean indicating whether the submission is a quarterly or monthly submission (True
                    if quarter)
                existing_submission: the existing submission to compare against

            Returns:
                The start and end dates in datetime format of the submission

            Raises:
                ResponseException: Required values were not provided, date was improperly formatted, start date is
                    after end date, quarterly submission does not span the required amount of time, or end month
                    is not a valid final month of a quarter
        """
        # if any of the date fields are none, there should be an existing submission otherwise, we shouldn't be here
        if None in (start_date, end_date, is_quarter) and existing_submission is None:
            raise ResponseException("An existing submission is required when start/end date "
                                    "or is_quarter aren't supplied", StatusCode.INTERNAL_ERROR)

        # Convert submission start/end dates from the request into Python date objects. If a date is missing, grab it
        # from the existing submission. Note: a previous check ensures that there's an existing submission when the
        # start/end dates are empty
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
            raise ResponseException("Date must be provided as MM/YYYY", StatusCode.CLIENT_ERROR, ValueError)

        # The front-end is doing date checks, but we'll also do a few server side to ensure everything is correct when
        # clients call the API directly
        if start_date > end_date:
            raise ResponseException(
                "Submission start date {} is after the end date {}".format(start_date, end_date),
                StatusCode.CLIENT_ERROR)

        # Currently, broker allows quarterly submissions for a single quarter only. the front-end handles this
        # requirement, but since we have some downstream logic that depends on a quarterly submission representing one
        # quarter, we'll check server side as well
        is_quarter = is_quarter if is_quarter is not None else existing_submission.is_quarter_format
        if is_quarter is None:
            is_quarter = existing_submission.is_quarter_format
        if is_quarter:
            if relativedelta(end_date + relativedelta(months=1), start_date).months != 3:
                raise ResponseException("Quarterly submission must span 3 months", StatusCode.CLIENT_ERROR)
            if end_date.month % 3 != 0:
                raise ResponseException(
                    "Invalid end month for a quarterly submission: {}".format(end_date.month), StatusCode.CLIENT_ERROR)

        # Change end_date date to the final date
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

            Args:
                job_id: the ID of the job

            Returns:
                A flask response object, if successful just contains key "success" with value True, otherwise value is
                False. If there was an error, returns a JsonResponse object containing the details of the error
        """
        sess = GlobalDB.db().session
        response_dict = {}
        try:
            # Compare user ID with user who submitted job, if no match return 400
            job = sess.query(Job).filter_by(job_id=job_id).one()
            submission = sess.query(Submission).filter_by(submission_id=job.submission_id).one()
            if (submission.d2_submission and not current_user_can_on_submission('editfabs', submission)) or \
                    (not submission.d2_submission and not current_user_can_on_submission('writer', submission)):
                # This user cannot finalize this job
                raise ResponseException("Cannot finalize a job for a different agency", StatusCode.CLIENT_ERROR)
            # Change job status to finished
            if job.job_type_id == JOB_TYPE_DICT["file_upload"]:
                mark_job_status(job_id, 'finished')
                response_dict["success"] = True
                response_dict["submission_id"] = job.submission_id
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

    def download_file(self, local_file_path, file_url, upload_name, response):
        """ Download a file locally from the specified URL.

            Args:
                local_file_path: path to where the local file will be uploaded
                file_url: the path to the file including the file name
                upload_name: name to upload the file as
                response: the response streamed to the application

            Returns:
                Boolean indicating if the file could be successfully downloaded

            Raises:
                ResponseException: Error if the file_url doesn't point to a valid file or the local_file_path is not
                    a valid directory
        """
        if not self.is_local:
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
        # Not a valid file
        elif not os.path.isfile(file_url):
            raise ResponseException('{} does not exist'.format(file_url), StatusCode.INTERNAL_ERROR)
        # Not a valid file path
        elif not os.path.isdir(os.path.dirname(local_file_path)):
            dirname = os.path.dirname(local_file_path)
            raise ResponseException('{} folder does not exist'.format(dirname), StatusCode.INTERNAL_ERROR)
        else:
            copyfile(file_url, local_file_path)
            return True

    def generate_file(self, submission, file_type, start, end):
        """ Start a file generation job for the specified file type within a submission

            Args:
                submission: submission for which we're generating the file
                file_type: type of file to generate the job for
                start: the start date for the file to generate
                end: the end date for the file to generate

            Returns:
                Results of check_generation or JsonResponse object containing an error if the prerequisite job isn't
                complete.
        """
        # if submission is a FABS submission, throw an error
        if submission.d2_submission:
            return JsonResponse.error(ValueError("Cannot generate files for FABS submissions"), StatusCode.CLIENT_ERROR)

        # if the file is D1 or D2 and we don't have start or end, raise an error
        if file_type in ['D1', 'D2'] and (not start or not end):
            return JsonResponse.error(ValueError("Must have a start and end date for D file generation"),
                                      StatusCode.CLIENT_ERROR)

        submission_id = submission.submission_id
        sess = GlobalDB.db().session
        job = sess.query(Job).filter(Job.submission_id == submission_id,
                                     Job.file_type_id == FILE_TYPE_DICT_LETTER_ID[file_type],
                                     Job.job_type_id == JOB_TYPE_DICT['file_upload']).one()

        log_data = {
            'message': 'Starting {} file generation within submission {}'.format(file_type, submission_id),
            'message_type': 'BrokerInfo',
            'submission_id': submission_id,
            'job_id': job.job_id,
            'file_type': file_type
        }
        logger.info(log_data)

        try:
            # Check prerequisites on upload job
            if not check_generation_prereqs(submission_id, file_type):
                raise ResponseException("Must wait for successful completion of prerequisite validation job",
                                        StatusCode.CLIENT_ERROR)
        except ResponseException as exc:
            return JsonResponse.error(exc, exc.status)

        success, error_response = start_generation_job(job, start, end)

        log_data['message'] = 'Finished start_generation_job method for submission {}'.format(submission_id)
        logger.debug(log_data)

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
                submission.updated_at = datetime.utcnow()
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
        """ Start a file generation job for the specified file type not connected to a submission

            Args:
                file_type: type of file to be generated
                cgac_code: the code of a CGAC agency if generating for a CGAC agency
                frec_code: the code of a FREC agency if generating for a FREC agency
                start: start date in a string, formatted MM/DD/YYYY
                end: end date in a string, formatted MM/DD/YYYY

            Returns:
                JSONResponse object with keys job_id, status, file_type, url, message, start, and end.

            Raises:
                ResponseException: if the start and end Strings cannot be parsed into dates
        """
        # Make sure it's a valid request
        if not cgac_code and not frec_code:
            return JsonResponse.error(ValueError("Detached file generation requires CGAC or FR Entity Code"),
                                      StatusCode.CLIENT_ERROR)

        # Check if date format is MM/DD/YYYY
        if not (StringCleaner.is_date(start) and StringCleaner.is_date(end)):
            raise ResponseException('Start or end date cannot be parsed into a date', StatusCode.CLIENT_ERROR)

        # Add job info
        file_type_name = FILE_TYPE_DICT_ID[FILE_TYPE_DICT_LETTER_ID[file_type]]
        new_job = self.add_generation_job_info(file_type_name=file_type_name, start_date=start, end_date=end)

        agency_code = frec_code if frec_code else cgac_code
        logger.info({
            'message': 'Starting detached {} file generation'.format(file_type),
            'message_type': 'BrokerInfo',
            'job_id': new_job.job_id,
            'file_type': file_type,
            'agency_code': agency_code,
            'start_date': start,
            'end_date': end
        })

        start_generation_job(new_job, start, end, agency_code)

        # Return same response as check generation route
        return self.check_detached_generation(new_job.job_id)

    def upload_fabs_file(self, fabs):
        """ Uploads the provided FABS file to S3 and creates a new submission if one doesn't exist or updates the
            existing submission if one does.

            Args:
                fabs: the FABS file being uploaded

            Returns:
                A JsonResponse containing the submission ID and a success boolean or a JsonResponse containing the
                details of the error that occurred.
        """
        if fabs is None:
            return JsonResponse.error(Exception('fabs field must be present and contain a file'),
                                      StatusCode.CLIENT_ERROR)
        sess = GlobalDB.db().session
        json_response, submission = None, None
        try:
            upload_files = []
            request_params = RequestDictionary.derive(self.request)
            logger.info({
                'message': 'Starting FABS file upload',
                'message_type': 'BrokerInfo',
                'agency_code': request_params.get('agency_code'),
                'existing_submission_id': request_params.get('existing_submission_id'),
                'file_name': fabs.filename
            })

            job_data = {}
            # Check for existing submission
            existing_submission_id = request_params.get('existing_submission_id')
            if existing_submission_id:
                existing_submission = True
                existing_submission_obj = sess.query(Submission).\
                    filter_by(submission_id=existing_submission_id).\
                    one()
                # If the existing submission is a DABS submission, stop everything
                if not existing_submission_obj.d2_submission:
                    raise ResponseException("Existing submission must be a FABS submission", StatusCode.CLIENT_ERROR)
                jobs = sess.query(Job).filter(Job.submission_id == existing_submission_id)
                # set all jobs to their initial status of "waiting"
                jobs[0].job_status_id = JOB_STATUS_DICT['waiting']
                sess.commit()
            else:
                existing_submission = None
                existing_submission_obj = None

            # Get cgac/frec codes for the given submission (or based on request params)
            if existing_submission_obj is not None:
                cgac_code = existing_submission_obj.cgac_code
                frec_code = existing_submission_obj.frec_code
            else:
                sub_tier_agency = sess.query(SubTierAgency).\
                    filter_by(sub_tier_agency_code=request_params['agency_code']).one()
                cgac_code = None if sub_tier_agency.is_frec else sub_tier_agency.cgac.cgac_code
                frec_code = sub_tier_agency.frec.frec_code if sub_tier_agency.is_frec else None

            # get the cgac code associated with this sub tier agency
            job_data["cgac_code"] = cgac_code
            job_data["frec_code"] = frec_code
            job_data["d2_submission"] = True
            job_data['reporting_start_date'] = None
            job_data['reporting_end_date'] = None

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
            self.build_file_map({'fabs': fabs}, ['fabs'], upload_files, submission)

            # Add jobs or update existing one
            job_dict = self.create_jobs_for_submission(upload_files, submission, existing_submission)

            filename_key = upload_files[0].upload_name
            bucket_name = CONFIG_BROKER["broker_files"] if self.is_local else CONFIG_BROKER["aws_bucket"]
            if CONFIG_BROKER['use_aws']:
                s3 = boto3.client('s3', region_name='us-gov-west-1')
                s3.upload_fileobj(fabs, bucket_name, filename_key)
            else:
                fabs.save(filename_key)
            json_response = self.finalize(job_dict["fabs_id"])
        except (ValueError, TypeError, NotImplementedError) as e:
            json_response = JsonResponse.error(e, StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            # call error route directly, status code depends on exception
            json_response = JsonResponse.error(e, e.status)
        except Exception as e:
            # unexpected exception, this is a 500 server error
            json_response = JsonResponse.error(e, StatusCode.INTERNAL_ERROR)
        finally:
            # handle a missing JSON response
            if json_response is None:
                json_response = JsonResponse.error(Exception("Failed to catch exception"), StatusCode.INTERNAL_ERROR)

            if json_response.status_code != StatusCode.OK and submission:
                jobs = sess.query(Job).filter(Job.submission_id == submission.submission_id,
                                              Job.job_type_id == JOB_TYPE_DICT['file_upload'],
                                              Job.job_status_id == JOB_STATUS_DICT['running'],
                                              Job.file_type_id == FILE_TYPE_DICT_LETTER_ID['FABS']).all()
                for job in jobs:
                    job.job_status_id = JOB_STATUS_DICT['failed']
                sess.commit()

            return json_response

    @staticmethod
    def check_detached_generation(job_id):
        """ Return information about detached file generation jobs

            Args:
                job_id: ID of the detached generation job

            Returns:
                Response object with keys job_id, status, file_type, url, message, start, and end.
        """
        response_dict = check_file_generation(job_id)

        return JsonResponse.create(StatusCode.OK, response_dict)

    @staticmethod
    def check_generation(submission, file_type):
        """ Return information about file generation jobs connected to a submission

            Args:
                submission: submission to get information from
                file_type: type of file being generated to check on

            Returns:
                Response object with keys status, file_type, url, message.
                If file_type is D1 or D2, also includes start and end.
        """
        sess = GlobalDB.db().session
        upload_job = sess.query(Job).filter(Job.submission_id == submission.submission_id,
                                            Job.file_type_id == FILE_TYPE_DICT_LETTER_ID[file_type],
                                            Job.job_type_id == JOB_TYPE_DICT['file_upload']).one()

        response_dict = check_file_generation(upload_job.job_id)

        return JsonResponse.create(StatusCode.OK, response_dict)

    @staticmethod
    def publish_fabs_submission(submission):
        """ Submits the FABS upload file associated with the submission ID, including processing all the derivations
            and updating relevant tables (such as un-caching all D2 files associated with this agency)

            Args:
                submission: submission to publish the file for

            Returns:
                A JsonResponse object containing the submission ID or the details of the error

            Raises:
                ResponseException: if the submission being published isn't a FABS submission, the submission is
                    already in the process of being published, or the submission has already been published
        """
        # Check to make sure it's a valid d2 submission who hasn't already started a publish process
        if not submission.d2_submission:
            raise ResponseException("Submission is not a FABS submission", StatusCode.CLIENT_ERROR)
        if submission.publish_status_id == PUBLISH_STATUS_DICT['publishing']:
            raise ResponseException("Submission is already publishing", StatusCode.CLIENT_ERROR)
        if submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished']:
            raise ResponseException("Submission has already been published", StatusCode.CLIENT_ERROR)

        # if it's an unpublished FABS submission, we can start the process
        sess = GlobalDB.db().session
        submission_id = submission.submission_id
        log_data = {
            'message': 'Starting FABS submission publishing',
            'message_type': 'BrokerDebug',
            'submission_id': submission_id
        }
        logger.info(log_data)

        # set publish_status to "publishing"
        sess.query(Submission).filter_by(submission_id=submission_id).\
            update({"publish_status_id": PUBLISH_STATUS_DICT['publishing'], "updated_at": datetime.utcnow()},
                   synchronize_session=False)
        sess.commit()

        try:
            # check to make sure no new entries have been published that collide with the new rows
            # (correction_delete_indicatr is not C or D)
            # need to set the models to something because the names are too long and flake gets mad
            dafa = DetachedAwardFinancialAssistance
            pafa = PublishedAwardFinancialAssistance
            colliding_rows = sess.query(dafa.afa_generated_unique). \
                filter(dafa.is_valid.is_(True),
                       dafa.submission_id == submission_id,
                       func.coalesce(func.upper(dafa.correction_delete_indicatr), '').notin_(['C', 'D'])).\
                join(pafa, and_(dafa.afa_generated_unique == pafa.afa_generated_unique, pafa.is_active.is_(True))).\
                count()
            if colliding_rows > 0:
                raise ResponseException("1 or more rows in this submission were already published (in a separate "
                                        "submission). This occurred in the time since your validations were completed. "
                                        "To prevent duplicate records, this submission must be revalidated in order to "
                                        "publish.",
                                        StatusCode.CLIENT_ERROR)

            # get all valid lines for this submission
            query = sess.query(DetachedAwardFinancialAssistance).\
                filter_by(is_valid=True, submission_id=submission_id).all()

            # Create lookup dictionaries so we don't have to query the API every time. We do biggest to smallest
            # to save the most possible space, although none of these should take that much.
            state_dict = {}
            country_dict = {}
            sub_tier_dict = {}
            cfda_dict = {}
            county_dict = {}
            fpds_office_dict = {}

            # This table is big enough that we want to only grab 2 columns
            offices = sess.query(FPDSContractingOffice.contracting_office_code,
                                 FPDSContractingOffice.contracting_office_name).all()
            for office in offices:
                fpds_office_dict[office.contracting_office_code] = office.contracting_office_name
            del offices

            counties = sess.query(CountyCode).all()
            for county in counties:
                # We ony ever get county name by state + code so we can make the keys a combination
                county_dict[county.state_code.upper() + county.county_number] = county.county_name
            del counties

            # Only grabbing the 2 columns we need because, unlike the other lookups, this has a ton of columns and
            # they can be pretty big
            cfdas = sess.query(CFDAProgram.program_number, CFDAProgram.program_title).all()
            for cfda in cfdas:
                # This is so the key is always "##.###", which is what's required based on the SQL
                # Could also be "###.###" which this will still pad correctly
                cfda_dict["%06.3f" % cfda.program_number] = cfda.program_title
            del cfdas

            sub_tiers = sess.query(SubTierAgency).all()
            for sub_tier in sub_tiers:
                sub_tier_dict[sub_tier.sub_tier_agency_code] = {
                    "is_frec": sub_tier.is_frec,
                    "cgac_code": sub_tier.cgac.cgac_code,
                    "frec_code": sub_tier.frec.frec_code,
                    "sub_tier_agency_name": sub_tier.sub_tier_agency_name,
                    "agency_name": sub_tier.frec.agency_name if sub_tier.is_frec else sub_tier.cgac.agency_name
                }
            del sub_tiers

            countries = sess.query(CountryCode).all()
            for country in countries:
                country_dict[country.country_code.upper()] = country.country_name
            del countries

            states = sess.query(States).all()
            for state in states:
                state_dict[state.state_code.upper()] = state.state_name
            del states

            agency_codes_list = []
            row_count = 1
            log_data['message'] = 'Starting derivations for FABS submission'
            logger.info(log_data)
            for row in query:
                # remove all keys in the row that are not in the intermediate table
                temp_obj = row.__dict__

                temp_obj.pop('row_number', None)
                temp_obj.pop('is_valid', None)
                temp_obj.pop('created_at', None)
                temp_obj.pop('updated_at', None)
                temp_obj.pop('_sa_instance_state', None)

                temp_obj = fabs_derivations(temp_obj, sess, state_dict, country_dict, sub_tier_dict, cfda_dict,
                                            county_dict, fpds_office_dict)

                # if it's a correction or deletion row and an old row is active, update the old row to be inactive
                if row.correction_delete_indicatr is not None:
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

                # update the list of affected agency_codes
                if temp_obj['awarding_agency_code'] not in agency_codes_list:
                    agency_codes_list.append(temp_obj['awarding_agency_code'])

                if row_count % 1000 == 0:
                    log_data['message'] = 'Completed derivations for {} rows'.format(row_count)
                    logger.info(log_data)
                row_count += 1

            # update all cached D2 FileRequest objects that could have been affected by the publish
            for agency_code in agency_codes_list:
                sess.query(FileRequest).\
                    filter(FileRequest.agency_code == agency_code,
                           FileRequest.is_cached_file.is_(True),
                           FileRequest.file_type == 'D2',
                           sa.or_(FileRequest.start_date <= submission.reporting_end_date,
                                  FileRequest.end_date >= submission.reporting_start_date)).\
                    update({"is_cached_file": False}, synchronize_session=False)
            sess.commit()
        except Exception as e:
            log_data['message'] = 'An error occurred while publishing a FABS submission'
            log_data['message_type'] = 'BrokerError'
            log_data['error_message'] = str(e)
            logger.error(log_data)

            # rollback the changes if there are any errors. We want to submit everything together
            sess.rollback()

            sess.query(Submission).filter_by(submission_id=submission_id). \
                update({"publish_status_id": PUBLISH_STATUS_DICT['unpublished'], "updated_at": datetime.utcnow()},
                       synchronize_session=False)
            sess.commit()

            # we want to return response exceptions in such a way that we can see the message, not catching it
            # separately because we still want to rollback the changes and set the status to unpublished
            if type(e) == ResponseException:
                return JsonResponse.error(e, e.status)

            return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)
        log_data['message'] = 'Completed derivations for FABS submission'
        logger.info(log_data)

        sess.query(Submission).filter_by(submission_id=submission_id).\
            update({"publish_status_id": PUBLISH_STATUS_DICT['published'], "certifying_user_id": g.user.user_id,
                    "updated_at": datetime.utcnow()}, synchronize_session=False)

        # create the certify_history entry
        certify_history = CertifyHistory(created_at=datetime.utcnow(), user_id=g.user.user_id,
                                         submission_id=submission_id)
        sess.add(certify_history)
        sess.commit()

        # get the certify_history entry including the PK
        certify_history = sess.query(CertifyHistory).filter_by(submission_id=submission_id).\
            order_by(CertifyHistory.created_at.desc()).first()

        # generate the published rows file and move all files
        # (locally we don't move but we still need to populate the certified_files_history table)
        FileHandler.move_certified_files(FileHandler, submission, certify_history, g.is_local)

        response_dict = {"submission_id": submission_id}
        return JsonResponse.create(StatusCode.OK, response_dict)

    def get_protected_files(self):
        """ Gets a set of urls to protected files (on S3 with timeouts) on the help page

            Returns:
                A JsonResponse object with the urls in a list or an empty object if local
        """
        response = {}
        if self.is_local:
            response["urls"] = {}
            return JsonResponse.create(StatusCode.CLIENT_ERROR, response)

        response["urls"] = self.s3manager.get_file_urls(bucket_name=CONFIG_BROKER["static_files_bucket"],
                                                        path=CONFIG_BROKER["help_files_path"])
        return JsonResponse.create(StatusCode.OK, response)

    @staticmethod
    def add_generation_job_info(file_type_name, job=None, start_date=None, end_date=None):
        """ Add details to jobs for generating files

            Args:
                file_type_name: the name of the file type being generated
                job: the generation job, None if it is a detached generation
                start_date: The start date for the generation job, only used for detached files
                end_date: The end date for the generation job, only used for detached files

            Returns:
                the file generation job
        """
        sess = GlobalDB.db().session

        # Create a new job for a detached generation
        if job is None:
            job = Job(job_type_id=JOB_TYPE_DICT['file_upload'], user_id=g.user.user_id,
                      file_type_id=FILE_TYPE_DICT[file_type_name], start_date=start_date, end_date=end_date)
            sess.add(job)

        # Update the job details
        job.message = None
        job.job_status_id = JOB_STATUS_DICT["ready"]
        sess.commit()
        sess.refresh(job)

        return job

    def build_file_map(self, file_dict, file_type_list, upload_files, submission):
        """ Build fileNameMap to be used in creating jobs

            Args:
                file_dict: parameters provided by the API request
                file_type_list: a list of all file types needed by a certain submission type
                upload_files: files that need to be uploaded
                submission: submission this file map is for

            Raises:
                ResponseException: If a new submission is being made but not all the file types in the file_type_list
                    are included in the request_params
        """
        for file_type in file_type_list:
            # if file_type not included in request, skip it, checks for validity are done before calling this
            if not file_dict.get(file_type):
                continue
            file_reference = file_dict.get(file_type)
            try:
                file_name = file_reference.filename
            except:
                return JsonResponse.error(Exception("{} parameter must be a file in binary form".format(file_type)),
                                          StatusCode.CLIENT_ERROR)
            if file_name:
                if not self.is_local:
                    upload_name = "{}/{}".format(
                        submission.submission_id,
                        S3Handler.get_timestamped_filename(file_name)
                    )
                else:
                    upload_name = os.path.join(self.server_path, S3Handler.get_timestamped_filename(file_name))

                upload_files.append(FileHandler.UploadFile(
                    file_type=file_type,
                    upload_name=upload_name,
                    file_name=file_name,
                    file_letter=FILE_TYPE_DICT_LETTER[FILE_TYPE_DICT[file_type]]
                ))

    @staticmethod
    def create_jobs_for_submission(upload_files, submission, existing_submission):
        """ Create the jobs for a submission or update existing ones.

            Args:
                upload_files: files to be uploaded
                submission: submission the dictionary is for
                existing_submission: boolean indicating if the submission is new or an existing one (true for existing)

            Returns:
                A dictionary containing the file types and jobs for those file types
        """
        file_job_dict = create_jobs(upload_files, submission, existing_submission)
        job_dict = {}
        for file_type in file_job_dict.keys():
            if "submission_id" not in file_type:
                job_dict[file_type + "_id"] = file_job_dict[file_type]
        return job_dict

    @staticmethod
    def restart_validation(submission, fabs):
        """ Restart validations for a submission

            Args:
                submission: the submission to restart the validations for
                fabs: a boolean to indicate whether the submission is a FABS or DABS submission (True for FABS)

            Returns:
                JsonResponse object with a "success" message
        """
        sess = GlobalDB.db().session
        # Determine which job types to start
        if not fabs:
            initial_file_types = [FILE_TYPE_DICT['appropriations'], FILE_TYPE_DICT['program_activity'],
                                  FILE_TYPE_DICT['award_financial']]
        else:
            initial_file_types = [FILE_TYPE_DICT['fabs']]

        jobs = sess.query(Job).filter(Job.submission_id == submission.submission_id).all()

        # set all jobs to their initial status of either "waiting" or "ready"
        for job in jobs:
            if job.job_type_id == JOB_TYPE_DICT["file_upload"] and \
               job.file_type_id in [FILE_TYPE_DICT["award"], FILE_TYPE_DICT["award_procurement"]]:
                # file generation handled on backend, mark as ready
                job.job_status_id = JOB_STATUS_DICT['ready']
                file_request = sess.query(FileRequest).filter_by(job_id=job.job_id).one_or_none()

                # uncache any related D file requests
                if file_request:
                    file_request.is_cached_file = False
                    if file_request.parent_job_id:
                        parent_file_request = sess.query(FileRequest).filter_by(job_id=file_request.parent_job_id).\
                            one_or_none()
                        if parent_file_request:
                            parent_file_request.is_cached_file = False
            else:
                # these are dependent on file D2 validation
                job.job_status_id = JOB_STATUS_DICT['waiting']

        # update upload jobs to "running" for files A, B, and C for DABS submissions or for the upload job in FABS
        upload_jobs = [job for job in jobs if job.job_type_id in [JOB_TYPE_DICT['file_upload']] and
                       job.file_type_id in initial_file_types]

        for job in upload_jobs:
            job.job_status_id = JOB_STATUS_DICT['running']
        sess.commit()

        # call finalize job for the upload jobs for files A, B, and C for DABS submissions and the only job for FABS,
        # which will kick off the rest of the process for DABS and indicate to the user that the validations are done
        # for FABS
        for job in upload_jobs:
            FileHandler.finalize(job.job_id)

        return JsonResponse.create(StatusCode.OK, {"message": "Success"})

    def move_certified_files(self, submission, certify_history, is_local):
        """ Copy all files within the ceritified submission to the correct certified files bucket/directory. FABS
            submissions also create a file containing all the published rows

            Args:
                submission: submission for which to move the files
                certify_history: a CertifyHistory object to use for timestamps and to update once the files are moved
                is_local: a boolean indicating whether the application is running locally or not
        """
        try:
            self.s3manager
        except AttributeError:
            self.s3manager = S3Handler()

        sess = GlobalDB.db().session
        submission_id = submission.submission_id
        log_data = {
            'message': 'Starting move_certified_files',
            'message_type': 'BrokerDebug',
            'submission_id': submission_id,
            'submission_type': 'FABS' if submission.d2_submission else 'DABS'
        }
        logger.debug(log_data)

        # get the list of upload jobs
        jobs = sess.query(Job).filter(Job.submission_id == submission_id,
                                      Job.job_type_id == JOB_TYPE_DICT['file_upload'],
                                      Job.filename.isnot(None)).all()
        original_bucket = CONFIG_BROKER['aws_bucket']
        new_bucket = CONFIG_BROKER['certified_bucket']
        agency_code = submission.cgac_code if submission.cgac_code else submission.frec_code

        # warning file doesn't apply to FABS submissions
        possible_warning_files = [FILE_TYPE_DICT["appropriations"], FILE_TYPE_DICT["program_activity"],
                                  FILE_TYPE_DICT["award_financial"]]

        # set the route within the bucket
        if submission.d2_submission:
            created_at_date = certify_history.created_at
            route_vars = ["FABS", agency_code, created_at_date.year, '{:02d}'.format(created_at_date.month)]
        else:
            route_vars = [agency_code, submission.reporting_fiscal_year, submission.reporting_fiscal_period // 3,
                          certify_history.certify_history_id]
        new_route = '/'.join([str(var) for var in route_vars]) + '/'

        for job in jobs:
            log_data['job_id'] = job.job_id

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
                    # create names and move warning file
                    warning_file_name = report_file_name(submission_id, True, job.file_type.name)
                    warning_file = new_route + warning_file_name
                    self.s3manager.copy_file(original_bucket=original_bucket, new_bucket=new_bucket,
                                             original_path="errors/" + warning_file_name, new_path=warning_file)
                else:
                    warning_file = CONFIG_SERVICES['error_report_path'] + report_file_name(submission_id, True,
                                                                                           job.file_type.name)

            narrative = None
            if submission.d2_submission:
                # FABS published submission, create the FABS published rows file
                log_data['message'] = 'Generating published FABS file from publishable rows'
                logger.info(log_data)
                new_path = create_fabs_published_file(sess, submission_id, new_route)
            else:
                # DABS certified submission
                # get the narrative relating to the file
                narrative = sess.query(SubmissionNarrative).\
                    filter_by(submission_id=submission_id, file_type_id=job.file_type_id).one_or_none()
                if narrative:
                    narrative = narrative.narrative

                # only actually move the files if it's not a local submission
                if not is_local:
                    self.s3manager.copy_file(original_bucket=original_bucket, new_bucket=new_bucket,
                                             original_path=job.filename, new_path=new_path)

            # create the certified_files_history for this file
            file_history = CertifiedFilesHistory(certify_history_id=certify_history.certify_history_id,
                                                 submission_id=submission_id, file_type_id=job.file_type_id,
                                                 filename=new_path, narrative=narrative,
                                                 warning_filename=warning_file)
            sess.add(file_history)

        # FABS submissions don't have cross-file validations
        if not submission.d2_submission:
            cross_list = {"B": "A", "C": "B", "D1": "C", "D2": "C"}
            for key, value in cross_list.items():
                first_file = FILE_TYPE_DICT_LETTER_NAME[value]
                second_file = FILE_TYPE_DICT_LETTER_NAME[key]

                # create warning file path
                if not is_local:
                    warning_file_name = report_file_name(submission_id, True, first_file, second_file)
                    warning_file = new_route + warning_file_name

                    # move the file if we aren't local
                    self.s3manager.copy_file(original_bucket=original_bucket, new_bucket=new_bucket,
                                             original_path="errors/" + warning_file_name, new_path=warning_file)
                else:
                    warning_file = CONFIG_SERVICES['error_report_path'] + report_file_name(submission_id, True,
                                                                                           first_file, second_file)

                # add certified history
                file_history = CertifiedFilesHistory(certify_history_id=certify_history.certify_history_id,
                                                     submission_id=submission_id, filename=None, file_type_id=None,
                                                     narrative=None, warning_filename=warning_file)
                sess.add(file_history)
        sess.commit()

        log_data['message'] = 'Completed move_certified_files'
        logger.debug(log_data)


def check_generation_prereqs(submission_id, file_type):
    """ Make sure the prerequisite jobs for this file type are complete without errors.

        Args:
            submission_id: the submission id for which we're checking file generation prerequisites
            file_type: the type of file being generated

        Returns:
            A boolean indicating if the job has no incomplete prerequisites (True if the job is clear to start)
    """

    sess = GlobalDB.db().session
    prereq_query = sess.query(Job).filter(Job.submission_id == submission_id,
                                          or_(Job.job_status_id != JOB_STATUS_DICT['finished'],
                                              Job.number_of_errors > 0))

    # Check cross-file validation if generating E or F
    if file_type in ['E', 'F']:
        unfinished_prereqs = prereq_query.filter(Job.job_type_id == JOB_TYPE_DICT['validation']).count()
    # Check A, B, C files if generating a D file
    elif file_type in ['D1', 'D2']:
        unfinished_prereqs = prereq_query.filter(Job.file_type_id.in_([FILE_TYPE_DICT['appropriations'],
                                                                       FILE_TYPE_DICT['program_activity'],
                                                                       FILE_TYPE_DICT['award_financial']])).count()
    else:
        raise ResponseException('Invalid type for file generation', StatusCode.CLIENT_ERROR)

    return unfinished_prereqs == 0


def narratives_for_submission(submission):
    """ Fetch narratives for this submission, indexed by file letter

        Args:
            submission: the submission to gather narratives for

        Returns:
            JsonResponse object with the contents of the narratives in a key/value pair of letter/narrative
    """
    sess = GlobalDB.db().session
    result = {letter: '' for letter in FILE_TYPE_DICT_LETTER.values() if letter != 'FABS'}
    narratives = sess.query(SubmissionNarrative).filter_by(submission_id=submission.submission_id)
    for narrative in narratives:
        letter = FILE_TYPE_DICT_LETTER[narrative.file_type_id]
        result[letter] = narrative.narrative
    return JsonResponse.create(StatusCode.OK, result)


def update_narratives(submission, narrative_request):
    """ Clear existing narratives and replace them with the provided set.

        Args:
            submission: submission to update the narratives for
            narrative_request: the contents of the request from the API
    """
    json = narrative_request or {}
    # clean input
    narratives_json = {key.upper(): value.strip() for key, value in json.items()
                       if isinstance(value, str) and value.strip()}

    sess = GlobalDB.db().session
    # Delete old narratives
    sess.query(SubmissionNarrative).filter_by(submission_id=submission.submission_id).\
        delete(synchronize_session='fetch')     # fetch just in case

    narratives = []
    for file_type_id, letter in FILE_TYPE_DICT_LETTER.items():
        if letter in narratives_json and letter != 'FABS':
            narratives.append(SubmissionNarrative(
                submission_id=submission.submission_id,
                file_type_id=file_type_id,
                narrative=narratives_json[letter]
            ))
    sess.add_all(narratives)
    sess.commit()

    return JsonResponse.create(StatusCode.OK, {})


def create_fabs_published_file(sess, submission_id, new_route):
    """ Create a file containing all the published rows from this submission_id

        Args:
            sess: the current DB session
            submission_id: ID of the submission the file is being created for
            new_route: the path to the new file

        Returns:
            The full path to the newly created/uploaded file
    """
    # create timestamped name and paths
    timestamped_name = S3Handler.get_timestamped_filename('submission_{}_published_fabs.csv'.format(submission_id))
    local_filename = "".join([CONFIG_BROKER['broker_files'], timestamped_name])
    upload_name = "".join([new_route, timestamped_name])

    # write file and stream to S3
    write_query_to_file(local_filename, upload_name, [key for key in fileD2.mapping], "published FABS", g.is_local,
                        published_fabs_query, {"sess": sess, "submission_id": submission_id}, is_certified=True)
    return local_filename if g.is_local else upload_name


def published_fabs_query(data_utils, page_start, page_end):
    """ Get the data from the published FABS table to write to the file with

        Args:
            data_utils: A dictionary of utils that are needed for the query being made, in this case including the
                session object and the submission ID
            page_start: the start of the slice to limit the data
            page_end: the end of the slice to limit the data

        Returns:
            A list of published FABS rows.
    """
    return fileD2.query_published_fabs_data(data_utils["sess"], data_utils["submission_id"], page_start, page_end).all()


def submission_to_dict_for_status(submission):
    """ Convert a Submission model into a dictionary, ready to be serialized as JSON for the get_status function

        Args:
            submission: submission to be converted to a dictionary

        Returns:
            A dictionary of submission information.
    """
    sess = GlobalDB.db().session

    number_of_rows = sess.query(func.sum(Job.number_of_rows)).\
        filter_by(submission_id=submission.submission_id).\
        scalar() or 0

    # @todo replace with a relationship
    # Determine the agency name
    cgac = sess.query(CGAC).filter_by(cgac_code=submission.cgac_code).one_or_none()
    frec = sess.query(FREC).filter_by(frec_code=submission.frec_code).one_or_none()
    if cgac:
        agency_name = cgac.agency_name
    elif frec:
        agency_name = frec.agency_name
    else:
        agency_name = ''

    relevant_job_types = (JOB_TYPE_DICT['csv_record_validation'], JOB_TYPE_DICT['validation'])
    relevant_jobs = sess.query(Job).filter(Job.submission_id == submission.submission_id,
                                           Job.job_type_id.in_(relevant_job_types))

    revalidation_threshold = sess.query(RevalidationThreshold).one_or_none()
    last_validated = get_last_validated_date(submission.submission_id)

    fabs_meta = get_fabs_meta(submission.submission_id) if submission.d2_submission else None

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
        # Broker allows submission for a single quarter or a single month, so reporting_period start and end dates
        # reported by check_status are always equal
        'reporting_period_start_date': reporting_date(submission),
        'reporting_period_end_date': reporting_date(submission),
        'jobs': [job_to_dict(job) for job in relevant_jobs],
        'publish_status': submission.publish_status.name,
        'quarterly_submission': submission.is_quarter_format,
        'fabs_meta': fabs_meta
    }


def get_status(submission, file_type=''):
    """ Get status information of all jobs in the submission specified in request object

        Args:
            submission: submission to get information for
            file_type: the type of job to get the status for; Default ''

        Returns:
            A flask response object to be sent back to client, holds a JSON where each file type (or the requested type)
            is a key to an object that holds status, has_errors, has_warnings, and message. If the user requests an
            invalid file type or the type requested is not valid for the submission type, returns a JSON response with
            a client error.
    """
    sess = GlobalDB.db().session
    file_type = file_type.lower()

    # Make sure the file type provided is valid
    if file_type and file_type not in FILE_TYPE_DICT and file_type != 'cross':
        return JsonResponse.error(ValueError(file_type + ' is not a valid file type'), StatusCode.CLIENT_ERROR)

    # Make sure the file type provided is valid for the submission type
    is_fabs = submission.d2_submission
    if file_type and (is_fabs and file_type != 'fabs') or (not is_fabs and file_type == 'fabs'):
        return JsonResponse.error(ValueError(file_type + ' is not a valid file type for this submission'),
                                  StatusCode.CLIENT_ERROR)

    # Set up a dictionary to store the jobs we want to look at and limit it to only the file types we care about. Also
    # setting up the response dict here because we need the same keys.
    response_template = {'status': 'ready', 'has_errors': False, 'has_warnings': False, 'message': ''}
    job_dict = {}
    response_dict = {}

    if file_type:
        job_dict[file_type] = []
        response_dict[file_type] = response_template
    elif is_fabs:
        job_dict['fabs'] = []
        response_dict['fabs'] = response_template
    else:
        # dabs submissions have all types except fabs including cross (which is not listed so we have to specify)
        job_dict['cross'] = []
        response_dict['cross'] = response_template.copy()
        for ft in FILE_TYPE_DICT:
            if ft != 'fabs':
                job_dict[ft] = []
                response_dict[ft] = response_template.copy()

    # We don't need to filter on file type, that will be handled by the dictionaries
    all_jobs = sess.query(Job).filter_by(submission_id=submission.submission_id)

    for job in all_jobs:
        dict_key = 'cross'
        if job.file_type:
            dict_key = job.file_type_name

        # we only want to insert the relevant jobs, the rest get ignored
        if dict_key in job_dict:
            job_dict[dict_key].append({
                'job_id': job.job_id,
                'job_status': job.job_status_id,
                'job_type': job.job_type_id,
                'error_message': job.error_message,
                'errors': job.number_of_errors,
                'warnings': job.number_of_warnings
            })

    for job_file_type, job_data in job_dict.items():
        response_dict[job_file_type] = process_job_status(job_data, response_dict[job_file_type])

    return JsonResponse.create(StatusCode.OK, response_dict)


def process_job_status(jobs, response_content):
    """ Process the status of a job type provided and update the response content provided with the new information.

        Args:
            jobs: An array of jobs with one or two jobs referring to the same file type (upload, validation, or both)
                Must contain all the jobs associated with that file type
            response_content: the skeleton object holding the information that needs to be returned (status, has_errors,
                has_warnings, and message)

        Returns:
            The response_content object originally provided, updated based on what the actual status of the jobs is.
    """
    upload = None
    validation = None
    upload_status = ''
    validation_status = ''
    for job in jobs:
        if job['job_type'] == JOB_TYPE_DICT['file_upload']:
            upload = job
            upload_status = JOB_STATUS_DICT_ID[job['job_status']]
        else:
            validation = job
            validation_status = JOB_STATUS_DICT_ID[job['job_status']]

    # checking for failures
    if upload_status == 'invalid' or upload_status == 'failed' or validation_status == 'failed':
        response_content['status'] = 'failed'
        response_content['has_errors'] = True
        response_content['message'] = upload['error_message'] or validation['error_message'] or ''
        return response_content

    if validation_status == 'invalid':
        response_content['status'] = 'finished'
        response_content['has_errors'] = True
        response_content['message'] = upload['error_message'] or validation['error_message'] or ''
        return response_content

    # If upload job exists and hasn't started or if it doesn't exist and validation job hasn't started,
    # it should just be ready
    if upload_status == 'ready' or upload_status == 'waiting' or \
            (upload_status == '' and (validation_status == 'ready' or validation_status == 'waiting')):
        return response_content

    # If the upload job is running, status is uploading
    if upload_status == 'running':
        response_content['status'] = 'uploading'
        return response_content

    # If the validation job is running, status is running
    if validation_status == 'running':
        response_content['status'] = 'running'
        return response_content

    # If both jobs are finished, figure out if there are errors
    response_content['status'] = 'finished'
    response_content['has_errors'] = validation is not None and validation['errors'] > 0
    response_content['has_warnings'] = validation is not None and validation['warnings'] > 0
    return response_content


def get_error_metrics(submission):
    """ Returns an Http response object containing error information for every validation job in specified submission

        Args:
            submission: submission to get error data for

        Returns:
            A JsonResponse object containing the error metrics for the submission or the details of the error
    """
    sess = GlobalDB.db().session
    return_dict = {}
    try:
        jobs = sess.query(Job).filter_by(submission_id=submission.submission_id)
        for job in jobs:
            # Get error metrics for all single-file validations
            if job.job_type.name == 'csv_record_validation':
                file_type = job.file_type.name
                data_list = get_error_metrics_by_job_id(job.job_id)
                return_dict[file_type] = data_list
        return JsonResponse.create(StatusCode.OK, return_dict)
    except (ValueError, TypeError) as e:
        return JsonResponse.error(e, StatusCode.CLIENT_ERROR)
    except ResponseException as e:
        return JsonResponse.error(e, e.status)
    except Exception as e:
        # Unexpected exception, this is a 500 server error
        return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)


def add_list_submission_filters(query, filters):
    """ Add provided filters to the list_submission query

        Args:
            query: already existing query to add to
            filters: provided filters

        Returns:
            The query updated with the valid provided filters

        Raises:
            ResponseException - invalid type is provided for one of the filters or the contents are invalid
    """
    sess = GlobalDB.db().session
    # Checking for submission ID filter
    if 'submission_ids' in filters:
        sub_list = filters['submission_ids']
        if sub_list and isinstance(sub_list, list):
            sub_list = [int(sub_id) for sub_id in sub_list]
            query = query.filter(Submission.submission_id.in_(sub_list))
        elif sub_list:
            raise ResponseException("submission_ids filter must be null or an array", StatusCode.CLIENT_ERROR)
    # Date range filter
    if 'last_modified_range' in filters:
        mod_dates = filters['last_modified_range']
        # last_modified_range must be a dict
        if mod_dates and isinstance(mod_dates, dict):
            start_date = mod_dates.get('start_date')
            end_date = mod_dates.get('end_date')

            # Make sure that, if it has content, start_date and end_date are both part of this filter
            if not start_date or not end_date:
                raise ResponseException("Both start_date and end_date must be provided", StatusCode.CLIENT_ERROR)

            # Start and end dates must be in the format MM/DD/YYYY and be
            if not (StringCleaner.is_date(start_date) and StringCleaner.is_date(end_date)):
                raise ResponseException("Start or end date cannot be parsed into a date of format MM/DD/YYYY",
                                        StatusCode.CLIENT_ERROR)
            # Make sure start date is not greater than end date (checking for >= because we add a day)
            start_date = datetime.strptime(start_date, '%m/%d/%Y')
            end_date = datetime.strptime(end_date, '%m/%d/%Y') + timedelta(days=1)
            if start_date >= end_date:
                raise ResponseException("Last modified start date cannot be greater than the end date",
                                        StatusCode.CLIENT_ERROR)

            query = query.filter(Submission.updated_at >= start_date, Submission.updated_at < end_date)
        elif mod_dates:
            raise ResponseException("last_modified_range filter must be null or an object", StatusCode.CLIENT_ERROR)
    # Agency code filter
    if 'agency_codes' in filters:
        agency_list = filters['agency_codes']
        if agency_list and isinstance(agency_list, list):
            # Split agencies into frec and cgac lists.
            cgac_list = [agency for agency in agency_list if isinstance(agency, str) and len(agency) == 3]
            frec_list = [agency for agency in agency_list if isinstance(agency, str) and len(agency) == 4]

            # If something isn't a length of 3 or 4, it's not valid and should instantly raise an exception
            if len(cgac_list) + len(frec_list) != len(agency_list):
                raise ResponseException("All codes in the agency_codes filter must be valid agency codes",
                                        StatusCode.CLIENT_ERROR)
            # If the number of CGACs or FRECs returned from a query using the codes doesn't match the length of
            # each list (ignoring duplicates) then something included wasn't a valid agency
            cgac_list = set(cgac_list)
            frec_list = set(frec_list)
            if (cgac_list and sess.query(CGAC).filter(CGAC.cgac_code.in_(cgac_list)).count() != len(cgac_list)) or \
                    (frec_list and sess.query(FREC).filter(FREC.frec_code.in_(frec_list)).count() != len(frec_list)):
                raise ResponseException("All codes in the agency_codes filter must be valid agency codes",
                                        StatusCode.CLIENT_ERROR)
            # We only want these filters in here if there's at least one CGAC or FREC to filter on
            agency_filters = []
            if len(cgac_list) > 0:
                agency_filters.append(CGAC.cgac_code.in_(cgac_list))
            if len(frec_list) > 0:
                agency_filters.append(FREC.frec_code.in_(frec_list))
            query = query.filter(or_(*agency_filters))
        elif agency_list:
            raise ResponseException("agency_codes filter must be null or an array", StatusCode.CLIENT_ERROR)
    # File name filter
    if 'file_names' in filters:
        file_list = filters['file_names']
        if file_list and isinstance(file_list, list):
            # Make a list of all the names we're filtering on
            file_array = []
            for file_name in file_list:
                file_regex = '.+\/.*' + str(file_name).upper() + '[^\/]*$'
                file_array.append(func.upper(Job.filename).op('~')(file_regex))

            # Create a subquery to get all submission IDs related to upload jobs (every type except cross-file has an
            # upload, just limiting jobs) that contain at least one of the file names listed.
            sub_query = sess.query(Job.submission_id.label('job_sub_id')).\
                filter(or_(*file_array)).\
                filter(Job.job_type_id == JOB_TYPE_DICT['file_upload']).\
                distinct().subquery()
            # Use the subquery to filter by those submission IDs.
            query = query.filter(Submission.submission_id.in_(sub_query))
        elif file_list:
            raise ResponseException("file_names filter must be null or an array", StatusCode.CLIENT_ERROR)
    return query


def list_submissions(page, limit, certified, sort='modified', order='desc', d2_submission=False, filters=None):
    """ List submission based on current page and amount to display. If provided, filter based on certification status

        Args:
            page: page number to use in getting the list
            limit: the number of entries per page
            certified: string indicating whether to display only certified, only uncertified, or both for submissions
            sort: the column to order on
            order: order ascending or descending
            d2_submission: boolean indicating whether it is a DABS or FABS submission (True if FABS)
            filters: an object containing the filters provided by the user

        Returns:
            Limited list of submissions and the total number of submissions the user has access to
    """
    sess = GlobalDB.db().session
    submission_updated_view = SubmissionUpdatedView()
    offset = limit * (page - 1)
    certifying_user = aliased(User)

    # List of all the columns to gather
    submission_columns = [Submission.submission_id, Submission.cgac_code, Submission.frec_code, Submission.user_id,
                          Submission.publish_status_id, Submission.d2_submission, Submission.number_of_warnings,
                          Submission.number_of_errors, Submission.updated_at, Submission.reporting_start_date,
                          Submission.reporting_end_date, Submission.certifying_user_id]
    cgac_columns = [CGAC.cgac_code, CGAC.agency_name.label('cgac_agency_name')]
    frec_columns = [FREC.frec_code, FREC.agency_name.label('frec_agency_name')]
    user_columns = [User.user_id, User.name, certifying_user.user_id.label('certifying_user_id'),
                    certifying_user.name.label('certifying_user_name')]
    view_columns = [submission_updated_view.submission_id, submission_updated_view.updated_at.label('updated_at')]
    sub_query = sess.query(CertifyHistory.submission_id, func.max(CertifyHistory.created_at).label('certified_date')).\
        group_by(CertifyHistory.submission_id).\
        subquery()

    columns_to_query = (submission_columns + cgac_columns + frec_columns + user_columns + view_columns +
                        [sub_query.c.certified_date])

    # Base query that is shared among all submission lists
    query = sess.query(*columns_to_query).\
        outerjoin(User, Submission.user_id == User.user_id).\
        outerjoin(certifying_user, Submission.certifying_user_id == certifying_user.user_id).\
        outerjoin(CGAC, Submission.cgac_code == CGAC.cgac_code).\
        outerjoin(FREC, Submission.frec_code == FREC.frec_code).\
        outerjoin(submission_updated_view.table, submission_updated_view.submission_id == Submission.submission_id).\
        outerjoin(sub_query, Submission.submission_id == sub_query.c.submission_id).\
        filter(Submission.d2_submission.is_(d2_submission))

    # Limit the data coming back to only what the given user is allowed to see
    if not g.user.website_admin:
        cgac_codes = [aff.cgac.cgac_code for aff in g.user.affiliations if aff.cgac]
        frec_codes = [aff.frec.frec_code for aff in g.user.affiliations if aff.frec]

        affiliation_filters = [Submission.user_id == g.user.user_id]
        if cgac_codes:
            affiliation_filters.append(Submission.cgac_code.in_(cgac_codes))
        if frec_codes:
            affiliation_filters.append(Submission.frec_code.in_(frec_codes))
        query = query.filter(sa.or_(*affiliation_filters))

    # Determine what types of submissions (published/unpublished/both) to display
    if certified != 'mixed':
        if certified == 'true':
            query = query.filter(Submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished'])
        else:
            query = query.filter(Submission.publish_status_id == PUBLISH_STATUS_DICT['unpublished'])

    # Add additional filters where applicable
    if filters:
        try:
            query = add_list_submission_filters(query, filters)
        except (ResponseException, ValueError) as e:
            return JsonResponse.error(e, StatusCode.CLIENT_ERROR)

    # Determine what to order by, default to "modified"
    options = {
        'modified': {'model': submission_updated_view, 'col': 'updated_at'},
        'reporting': {'model': Submission, 'col': 'reporting_start_date'},
        'agency': {'model': CGAC, 'col': 'agency_name'},
        'submitted_by': {'model': User, 'col': 'name'},
        'certified_date': {'model': sub_query.c, 'col': 'certified_date'}
    }

    if not options.get(sort):
        sort = 'modified'

    sort_order = getattr(options[sort]['model'], options[sort]['col'])

    # Determine how to sort agencies using FREC or CGAC name
    if sort == "agency":
        sort_order = case([
            (FREC.agency_name.isnot(None), FREC.agency_name),
            (CGAC.agency_name.isnot(None), CGAC.agency_name)
        ])

    # Set the sort order
    if order == 'desc':
        sort_order = sort_order.desc()

    query = query.order_by(sort_order)

    total_submissions = query.count()

    query = query.slice(offset, offset + limit)

    return JsonResponse.create(StatusCode.OK, {
        "submissions": [serialize_submission(submission) for submission in query],
        "total": total_submissions
    })


def list_certifications(submission):
    """ List all certifications for a single submission including the file history that goes with them.

        Args:
            submission: submission to get certifications for

        Returns:
            A JsonResponse containing a dictionary with the submission ID and a list of certifications or a JsonResponse
            error containing details of what went wrong.
    """
    if submission.d2_submission:
        return JsonResponse.error(ValueError("FABS submissions do not have a certification history"),
                                  StatusCode.CLIENT_ERROR)

    sess = GlobalDB.db().session

    certify_history = sess.query(CertifyHistory).filter_by(submission_id=submission.submission_id).\
        order_by(CertifyHistory.created_at.desc()).all()

    if len(certify_history) == 0:
        return JsonResponse.error(ValueError("This submission has no certification history"), StatusCode.CLIENT_ERROR)

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
    """ Get the signed URL for the specified historical file

        Args:
            submission: submission to get the file history for
            file_history_id: the CertifiedFilesHistory ID to get the file from
            is_warning: a boolean indicating if the file being retrieved is a warning or error file (True for warning)
            is_local: a boolean indicating if the application is being run locally (True for local)

        Returns:
            A JsonResponse containing a dictionary with the url to the file or a JsonResponse error containing details
            of what went wrong.
    """
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

        # Remove the last part of the array before putting it back together so we can use our existing functions
        filename = file_array.pop()
        file_path = '/'.join(x for x in file_array)
        url = S3Handler().get_signed_url(file_path, filename, bucket_route=CONFIG_BROKER['certified_bucket'],
                                         method="GET")

    return JsonResponse.create(StatusCode.OK, {"url": url})


def serialize_submission(submission):
    """ Convert the provided submission into a dictionary in a schema the frontend expects.

        Args:
            submission: the submission to convert to a dictionary

        Returns:
            A dictionary containing important details of the submission.
    """

    sess = GlobalDB.db().session

    jobs = sess.query(Job).filter_by(submission_id=submission.submission_id)
    files = get_submission_files(jobs)
    status = get_submission_status(submission, jobs)
    certified_on = get_lastest_certified_date(submission)
    agency_name = submission.cgac_agency_name if submission.cgac_agency_name else submission.frec_agency_name
    return {
        "submission_id": submission.submission_id,
        "last_modified": str(submission.updated_at),
        "status": status,
        "agency": agency_name if agency_name else 'N/A',
        "files": files,
        # @todo why are these a different format?
        "reporting_start_date": str(submission.reporting_start_date) if submission.reporting_start_date else None,
        "reporting_end_date": str(submission.reporting_end_date) if submission.reporting_end_date else None,
        "user": {"user_id": submission.user_id, "name": submission.name if submission.name else "No User"},
        "certifying_user": submission.certifying_user_name if submission.certifying_user_name else "",
        'publish_status': PUBLISH_STATUS_DICT_ID[submission.publish_status_id],
        "certified_on": str(certified_on) if certified_on else ""
    }


def submission_report_url(submission, warning, file_type, cross_type):
    """ Gets the signed URL for the specified error/warning file

        Args:
            submission: the submission to get the file for
            warning: a boolean indicating if the file is a warning or error file (True for warning)
            file_type: the name of the base file type of the error report
            cross_type: the name of the cross file type of the error report if applicable, else None

        Returns:
            A signed URL to S3 of the specified file when not run locally. The path to the file when run locally.
            If a cross file is requested and the pairing isn't valid, return a JsonResponse containing an error.
    """
    # If we're doing a cross-file url, make sure it's a valid pairing
    if cross_type:
        cross_pairs = {
            'program_activity': 'appropriations',
            'award_financial': 'program_activity',
            'award_procurement': 'award_financial',
            'award': 'award_financial'
        }
        if file_type != cross_pairs[cross_type]:
            return JsonResponse.error(ValueError("{} and {} is not a valid cross-pair.".format(file_type, cross_type)),
                                      StatusCode.CLIENT_ERROR)

    # Get the url
    file_name = report_file_name(submission.submission_id, warning, file_type, cross_type)
    if CONFIG_BROKER['local']:
        url = os.path.join(CONFIG_BROKER['broker_files'], file_name)
    else:
        url = S3Handler().get_signed_url("errors", file_name, method="GET")
    return JsonResponse.create(StatusCode.OK, {"url": url})


def get_upload_file_url(submission, file_type):
    """ Gets the signed url of the upload file for the given file type and submission.

        Args:
            submission: the submission to get the file url for
            file_type: the letter of the file type to get the file url for

        Returns:
            A signed URL to S3 of the specified file when not run locally. The path to the file when run locally.
            Error response if the wrong file type for the submission is given
    """
    # check for proper file type
    if (submission.d2_submission and file_type != 'FABS') or (not submission.d2_submission and file_type == 'FABS'):
        return JsonResponse.error(ValueError("Invalid file type for this submission"), StatusCode.CLIENT_ERROR)

    sess = GlobalDB.db().session
    file_job = sess.query(Job).filter(Job.submission_id == submission.submission_id,
                                      Job.file_type_id == FILE_TYPE_DICT_LETTER_ID[file_type],
                                      Job.job_type_id == JOB_TYPE_DICT['file_upload']).first()
    if not file_job.filename:
        return JsonResponse.error(ValueError("No file uploaded or generated for this type"), StatusCode.CLIENT_ERROR)

    split_name = file_job.filename.split('/')
    if CONFIG_BROKER['local']:
        # when local, can just grab the filename because it stores the entire path
        url = os.path.join(CONFIG_BROKER['broker_files'], split_name[-1])
    else:
        url = S3Handler().get_signed_url(split_name[0], split_name[1], method="GET")
    return JsonResponse.create(StatusCode.OK, {"url": url})


# TODO: Do we even use this anymore?
def get_xml_response_content(api_url):
    """ Retrieve XML Response from the provided API url.

        Args:
            api_url: API url to get the XML from

        Returns:
            XML from the provided url.
    """
    result = requests.get(api_url, verify=False, timeout=120).text
    logger.debug({
        'message': 'Result for {}: {}'.format(api_url, result),
        'function': 'get_xml_response_content'
    })
    return result
