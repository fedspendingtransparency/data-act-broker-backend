import boto3
import calendar
import logging
import os
import requests
import threading
import csv

from collections import namedtuple
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from flask import g, current_app
from sqlalchemy import func, and_, or_, Integer
from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import case, cast
from sqlalchemy.sql import extract

from dataactbroker.handlers.submission_handler import (create_submission, get_submission_status, get_submission_files,
                                                       get_submissions_in_period)
from dataactbroker.helpers.fabs_derivations_helper import fabs_derivations, log_derivation
from dataactbroker.helpers.filters_helper import permissions_filter, agency_filter
from dataactbroker.permissions import current_user_can_on_submission

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import create_jobs, mark_job_status, get_time_period

from dataactcore.models.domainModels import CGAC, FREC, SubTierAgency
from dataactcore.models.jobModels import (Job, Submission, Comment, SubmissionSubTierAffiliation, CertifyHistory,
                                          PublishHistory, PublishedFilesHistory, FileGeneration, FileType,
                                          CertifiedComment, generate_fiscal_year, generate_fiscal_period)
from dataactcore.models.lookups import (
    FILE_TYPE_DICT, FILE_TYPE_DICT_LETTER, FILE_TYPE_DICT_LETTER_ID, PUBLISH_STATUS_DICT, JOB_TYPE_DICT,
    JOB_STATUS_DICT, JOB_STATUS_DICT_ID, PUBLISH_STATUS_DICT_ID, FILE_TYPE_DICT_LETTER_NAME)
from dataactcore.models.stagingModels import DetachedAwardFinancialAssistance, PublishedAwardFinancialAssistance
from dataactcore.models.userModel import User
from dataactcore.models.views import SubmissionUpdatedView

from dataactcore.utils import fileD2
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.report import report_file_name
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.stringCleaner import StringCleaner

from dataactvalidator.filestreaming.csv_selection import write_stream_query
from dataactvalidator.validation_handlers.file_generation_manager import GEN_FILENAMES
from dataactvalidator.validation_handlers.validationManager import ValidationManager

logger = logging.getLogger(__name__)

ROWS_PER_LOOP = 10000
MIN_ROWS_LOG_BATCH = 100
LOG_BATCH_PERCENT = 10


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
    FILE_TYPES = ['appropriations', 'award_financial', 'program_activity']
    EXTERNAL_FILE_TYPES = ['D2', 'D1', 'E', 'F']
    VALIDATOR_RESPONSE_FILE = 'validatorResponse'

    UploadFile = namedtuple('UploadFile', ['file_type', 'upload_name', 'file_name', 'file_letter'])

    def __init__(self, route_request, is_local=False, server_path=''):
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

        # If both start and end date are provided, make sure monthly submissions are only one month long
        if not (start_date is None or end_date is None):
            formatted_start_date, formatted_end_date = FileHandler.check_submission_dates(start_date,
                                                                                          end_date, is_quarter)

            # Single period checks
            if not is_quarter:
                data = {
                    'message': 'A monthly submission must be exactly one month with the exception of periods 1 and 2,'
                               ' which must be selected together.'
                }
                period1 = 10
                period2 = 11

                # multiple years
                if not formatted_start_date.year == formatted_end_date.year:
                    return JsonResponse.create(StatusCode.CLIENT_ERROR, data)

                # Not the same month, not period 2
                if formatted_start_date.month != formatted_end_date.month and formatted_start_date.month != period1 \
                        and formatted_end_date.month != period2:
                    return JsonResponse.create(StatusCode.CLIENT_ERROR, data)

                # attempting to make just period 1 or period 2 submission without spanning both
                if (formatted_start_date.month == period1 and formatted_end_date.month != period2) or \
                        (formatted_start_date.month != period1 and formatted_end_date.month == period2):
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
            if '_files' in request_params and request_params['_files'].get(file_type):
                param_count += 1

        if not existing_submission_id and param_count != len(FileHandler.FILE_TYPES):
            raise ResponseException('Must include all files for a new submission', StatusCode.CLIENT_ERROR)

        if existing_submission_id and param_count == 0:
            raise ResponseException('Must include at least one file for an existing submission',
                                    StatusCode.CLIENT_ERROR)

        # Make sure all files are CSV or TXT files and not something else
        for file_type in request_params.get('_files'):
            file = request_params['_files'].get(file_type)
            extension = file.filename.split('.')[-1]
            if not extension or extension.lower() not in ['csv', 'txt']:
                raise ResponseException('All submitted files must be CSV or TXT format', StatusCode.CLIENT_ERROR)

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
                'cgac_code': 'cgac_code',
                'frec_code': 'frec_code',
                'reporting_period_start_date': 'reporting_start_date',
                'reporting_period_end_date': 'reporting_end_date',
                'is_quarter': 'is_quarter_format'}

            submission_data = {}
            existing_submission_id = request_params.get('existing_submission_id')
            if existing_submission_id:
                existing_submission = True
                existing_submission_obj = sess.query(Submission).filter_by(submission_id=existing_submission_id).one()
                # If the existing submission is a FABS submission, stop everything
                if existing_submission_obj.d2_submission:
                    raise ResponseException('Existing submission must be a DABS submission', StatusCode.CLIENT_ERROR)
                if existing_submission_obj.publish_status_id in (PUBLISH_STATUS_DICT['publishing'],
                                                                 PUBLISH_STATUS_DICT['reverting']):
                    raise ResponseException('Existing submission must not be publishing, certifying, or reverting',
                                            StatusCode.CLIENT_ERROR)
                jobs = sess.query(Job).filter(Job.submission_id == existing_submission_id)
                for job in jobs:
                    if job.job_status_id == JOB_STATUS_DICT['running']:
                        raise ResponseException('Submission already has a running job', StatusCode.CLIENT_ERROR)
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

            if not existing_submission:
                # Stripping off all extra whitespace so we don't create bad cgac/frec references. If it results in an
                # empty string, set it to None
                code_types = ['cgac_code', 'frec_code']
                for code in code_types:
                    submission_data[code] = submission_data[code].strip() if submission_data[code] else None
                    if submission_data[code] == '':
                        submission_data[code] = None

                cgac_code = submission_data['cgac_code'] or ''
                frec_code = submission_data['frec_code'] or ''
                if cgac_code != '' and frec_code != '':
                    raise ResponseException('New DABS submissions must have either a CGAC or a FREC code but not both',
                                            StatusCode.CLIENT_ERROR)
                if cgac_code == '' and frec_code == '':
                    raise ResponseException('New DABS submissions must have either a CGAC or a FREC code',
                                            StatusCode.CLIENT_ERROR)

            # make sure submission dates are valid
            formatted_start_date, formatted_end_date = FileHandler.check_submission_dates(
                submission_data.get('reporting_start_date'),
                submission_data.get('reporting_end_date'),
                str(submission_data.get('is_quarter_format')).upper() == 'TRUE',
                existing_submission_obj)
            submission_data['reporting_start_date'] = formatted_start_date
            submission_data['reporting_end_date'] = formatted_end_date
            if submission_data.get('is_quarter_format'):
                submission_data['is_quarter_format'] = (str(submission_data.get('is_quarter_format')).upper() == 'TRUE')

            reporting_fiscal_period = generate_fiscal_period(submission_data['reporting_end_date'])
            reporting_fiscal_year = generate_fiscal_year(submission_data['reporting_end_date'])

            test_submission = request_params.get('test_submission')
            test_submission = str(test_submission).upper() == 'TRUE'

            # set published_submission_ids for new submissions
            if not existing_submission:
                pub_subs = get_submissions_in_period(submission_data['cgac_code'], submission_data['frec_code'],
                                                     reporting_fiscal_year, reporting_fiscal_period,
                                                     submission_data['is_quarter_format'], filter_published='published')
                submission_data['published_submission_ids'] = [pub_sub.submission_id for pub_sub in pub_subs]
                # If there are already published submissions in this period/quarter or if it's a quarterly submission
                # that is FY22 or later (starting FY22 all submissions must be monthly), force the new submission to be
                # a test
                if len(submission_data['published_submission_ids']) > 0 or\
                        (reporting_fiscal_year >= 2022 and submission_data['is_quarter_format']):
                    test_submission = True

            submission = create_submission(g.user.user_id, submission_data, existing_submission_obj, test_submission)
            sess.add(submission)
            sess.commit()

            # build fileNameMap to be used in creating jobs
            file_dict = request_params['_files']
            self.build_file_map(file_dict, FileHandler.FILE_TYPES, upload_files, submission)

            if not existing_submission:
                # don't add external files to existing submission
                for ext_file_type in FileHandler.EXTERNAL_FILE_TYPES:
                    filename = GEN_FILENAMES[ext_file_type]
                    if ext_file_type in ['D1', 'D2']:
                        # default to using awarding agency and the start/end dates
                        filename = filename.format(formatted_start_date.strftime('%Y%m%d'),
                                                   formatted_end_date.strftime('%Y%m%d'), 'awarding', 'csv')
                    if not self.is_local:
                        upload_name = '{}/{}'.format(submission.submission_id,
                                                     S3Handler.get_timestamped_filename(filename))
                    else:
                        upload_name = filename

                    upload_files.append(FileHandler.UploadFile(
                        file_type=FILE_TYPE_DICT_LETTER_NAME[ext_file_type],
                        upload_name=upload_name,
                        file_name=filename,
                        file_letter=ext_file_type
                    ))

            # Add jobs or update existing ones
            job_dict = self.create_jobs_for_submission(upload_files, submission, existing_submission)

            def upload(file_ref, file_type, app, current_user, submission_id):
                filename_key = [x.upload_name for x in upload_files if x.file_type == file_type][0]
                bucket_name = CONFIG_BROKER['broker_files'] if self.is_local else CONFIG_BROKER['aws_bucket']
                logger.info({
                    'message': 'Uploading {}'.format(filename_key),
                    'message_type': 'BrokerInfo',
                    'file_type': file_type,
                    'submission_id': submission_id,
                    'file_name': filename_key
                })
                if CONFIG_BROKER['use_aws']:
                    s3 = boto3.client('s3', region_name='us-gov-west-1')
                    extra_args = {'Metadata': {'email': current_user.email}}
                    s3.upload_fileobj(file_ref, bucket_name, filename_key, ExtraArgs=extra_args)
                else:
                    file_ref.save(filename_key)
                logger.info({
                    'message': 'Uploaded {}'.format(filename_key),
                    'message_type': 'BrokerInfo',
                    'file_type': file_type,
                    'submission_id': submission_id,
                    'file_name': filename_key
                })
                with app.app_context():
                    g.user = current_user
                    self.finalize(job_dict[file_type + '_id'])
            for file_type, file_ref in request_params['_files'].items():
                t = threading.Thread(target=upload, args=(file_ref, file_type,
                                                          current_app._get_current_object(), g.user,
                                                          submission.submission_id))
                t.start()
                t.join()
            api_response = {'success': 'true', 'submission_id': submission.submission_id}
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
                json_response = JsonResponse.error(Exception('Failed to catch exception'), StatusCode.INTERNAL_ERROR)

            # handle errors within upload jobs
            if json_response.status_code != StatusCode.OK and submission:
                jobs = sess.query(Job).filter(Job.submission_id == submission.submission_id,
                                              Job.job_type_id == JOB_TYPE_DICT['file_upload'],
                                              Job.job_status_id == JOB_STATUS_DICT['running'],
                                              Job.file_type_id.in_([FILE_TYPE_DICT_LETTER_ID['A'],
                                                                    FILE_TYPE_DICT_LETTER_ID['B'],
                                                                    FILE_TYPE_DICT_LETTER_ID['C']])).all()
                for job in jobs:
                    job.job_status_id = JOB_STATUS_DICT['failed']
                    job.error_message = json_response.response[0].decode('utf-8')
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
            raise ResponseException('An existing submission is required when start/end date '
                                    'or is_quarter aren\'t supplied', StatusCode.INTERNAL_ERROR)

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
            raise ResponseException('Date must be provided as MM/YYYY', StatusCode.CLIENT_ERROR, ValueError)

        # The front-end is doing date checks, but we'll also do a few server side to ensure everything is correct when
        # clients call the API directly
        if start_date > end_date:
            raise ResponseException(
                'Submission start date {} is after the end date {}'.format(start_date, end_date),
                StatusCode.CLIENT_ERROR)

        # Currently, broker allows quarterly submissions for a single quarter only. the front-end handles this
        # requirement, but since we have some downstream logic that depends on a quarterly submission representing one
        # quarter, we'll check server side as well
        is_quarter = is_quarter if is_quarter is not None else existing_submission.is_quarter_format
        if is_quarter is None:
            is_quarter = existing_submission.is_quarter_format
        if is_quarter:
            if relativedelta(end_date + relativedelta(months=1), start_date).months != 3:
                raise ResponseException('Quarterly submission must span 3 months', StatusCode.CLIENT_ERROR)
            if end_date.month % 3 != 0:
                raise ResponseException(
                    'Invalid end month for a quarterly submission: {}'.format(end_date.month), StatusCode.CLIENT_ERROR)

        # Change end_date date to the final date
        end_date = datetime.strptime(str(end_date.year) + '/' + str(end_date.month) + '/'
                                     + str(calendar.monthrange(end_date.year, end_date.month)[1]), '%Y/%m/%d').date()

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
                raise ResponseException('Cannot finalize a job for a different agency', StatusCode.CLIENT_ERROR)
            # Change job status to finished
            if job.job_type_id == JOB_TYPE_DICT['file_upload']:
                mark_job_status(job_id, 'finished')
                response_dict['success'] = True
                response_dict['submission_id'] = job.submission_id
                return JsonResponse.create(StatusCode.OK, response_dict)
            else:
                raise ResponseException('Wrong job type for finalize route', StatusCode.CLIENT_ERROR)

        except (ValueError, TypeError) as e:
            return JsonResponse.error(e, StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            return JsonResponse.error(e, e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)

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
            # Make sure they only pass in csv or plain text files
            extension = fabs.filename.split('.')[-1]
            if not extension or extension.lower() not in ['csv', 'txt']:
                raise ValueError('FABS files must be CSV or TXT format')
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
            test_submission = str(request_params.get('test_submission')).upper() == 'TRUE'
            if existing_submission_id:
                existing_submission = True
                existing_submission_obj = sess.query(Submission).\
                    filter_by(submission_id=existing_submission_id).\
                    one()
                # If the existing submission is a DABS submission, stop everything
                if not existing_submission_obj.d2_submission:
                    raise ResponseException('Existing submission must be a FABS submission', StatusCode.CLIENT_ERROR)
                jobs = sess.query(Job).filter(Job.submission_id == existing_submission_id)
                if existing_submission_obj.publish_status_id != PUBLISH_STATUS_DICT['unpublished']:
                    raise ResponseException('FABS submission has already been published', StatusCode.CLIENT_ERROR)
                for job in jobs:
                    if job.job_status_id == JOB_STATUS_DICT['running']:
                        raise ResponseException('Submission already has a running job', StatusCode.CLIENT_ERROR)

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
            job_data['cgac_code'] = cgac_code
            job_data['frec_code'] = frec_code
            job_data['d2_submission'] = True
            job_data['reporting_start_date'] = None
            job_data['reporting_end_date'] = None

            submission = create_submission(g.user.user_id, job_data, existing_submission_obj, test_submission)
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
            bucket_name = CONFIG_BROKER['broker_files'] if self.is_local else CONFIG_BROKER['aws_bucket']
            logger.info({
                'message': 'Uploading {}'.format(filename_key),
                'message_type': 'BrokerInfo',
                'file_type': 'fabs',
                'submission_id': submission.submission_id,
                'file_name': filename_key
            })
            if CONFIG_BROKER['use_aws']:
                s3 = boto3.client('s3', region_name='us-gov-west-1')
                extra_args = {'Metadata': {'email': g.user.email}}
                s3.upload_fileobj(fabs, bucket_name, filename_key, ExtraArgs=extra_args)
            else:
                fabs.save(filename_key)
            logger.info({
                'message': 'Uploaded {}'.format(filename_key),
                'message_type': 'BrokerInfo',
                'file_type': 'fabs',
                'submission_id': submission.submission_id,
                'file_name': filename_key
            })
            json_response = self.finalize(job_dict['fabs_id'])
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
                json_response = JsonResponse.error(Exception('Failed to catch exception'), StatusCode.INTERNAL_ERROR)

            if json_response.status_code != StatusCode.OK and submission:
                fabs_job = sess.query(Job).filter(Job.submission_id == submission.submission_id,
                                                  Job.job_type_id == JOB_TYPE_DICT['file_upload'],
                                                  Job.job_status_id == JOB_STATUS_DICT['running'],
                                                  Job.file_type_id == FILE_TYPE_DICT_LETTER_ID['FABS']).one_or_none()
                if fabs_job:
                    fabs_job.job_status_id = JOB_STATUS_DICT['failed']
                    fabs_job.error_message = json_response.get('json', {}).get('message', '')
                sess.commit()

            return json_response

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
            raise ResponseException('Submission is not a FABS submission', StatusCode.CLIENT_ERROR)
        if submission.publish_status_id == PUBLISH_STATUS_DICT['publishing']:
            raise ResponseException('Submission is already publishing', StatusCode.CLIENT_ERROR)
        if submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished']:
            raise ResponseException('Submission has already been published', StatusCode.CLIENT_ERROR)
        if submission.test_submission:
            raise ValueError('Test submissions cannot be published')

        sess = GlobalDB.db().session
        submission_id = submission.submission_id
        # Check to make sure all jobs are finished
        unfinished_jobs = sess.query(Job).filter(Job.submission_id == submission_id,
                                                 Job.job_status_id != JOB_STATUS_DICT['finished']).count()
        if unfinished_jobs > 0:
            raise ResponseException('Submission has unfinished jobs and cannot be published', StatusCode.CLIENT_ERROR)

        # if it's an unpublished FABS submission that has only finished jobs, we can start the process
        log_derivation('Starting FABS submission publishing', submission_id)

        # set publish_status to "publishing"
        sess.query(Submission).filter_by(submission_id=submission_id).\
            update({'publish_status_id': PUBLISH_STATUS_DICT['publishing'], 'updated_at': datetime.utcnow()},
                   synchronize_session=False)
        sess.commit()

        try:
            # need to set the models to something because the names are too long and flake gets mad
            dafa = DetachedAwardFinancialAssistance
            pafa = PublishedAwardFinancialAssistance

            # Check to make sure no rows are currently publishing that collide with the rows in this submission
            # (in any way, including C or D)
            valid_sub_rows = sess.query(dafa.afa_generated_unique).\
                filter(dafa.submission_id == submission_id, dafa.is_valid.is_(True)).cte('valid_sub_rows')
            publishing_subs = sess.query(dafa.submission_id).\
                join(valid_sub_rows,
                     func.upper(valid_sub_rows.c.afa_generated_unique) == func.upper(dafa.afa_generated_unique)).\
                join(Submission, Submission.submission_id == dafa.submission_id).\
                filter(dafa.is_valid.is_(True),
                       dafa.submission_id != submission_id,
                       Submission.publish_status_id == PUBLISH_STATUS_DICT['publishing']).distinct().all()
            if publishing_subs:
                sub_list = []
                for sub in publishing_subs:
                    sub_list.append(str(sub.submission_id))
                raise ResponseException('1 or more rows in this submission are currently publishing (in a separate '
                                        'submission). To prevent duplicate records, please wait for the other '
                                        'submission(s) to finish publishing before trying to publish. IDs of '
                                        'submissions affecting this publish attempt: {}'.format(', '.join(sub_list)),
                                        StatusCode.CLIENT_ERROR)

            # check to make sure no new entries have been published that collide with the new rows
            # (correction_delete_indicatr is not C or D)
            colliding_rows = sess.query(dafa.afa_generated_unique). \
                filter(dafa.is_valid.is_(True),
                       dafa.submission_id == submission_id,
                       func.coalesce(func.upper(dafa.correction_delete_indicatr), '').notin_(['C', 'D'])).\
                join(pafa, and_(func.upper(dafa.afa_generated_unique) == func.upper(pafa.afa_generated_unique),
                                pafa.is_active.is_(True))).\
                count()
            if colliding_rows > 0:
                raise ResponseException('1 or more rows in this submission were already published (in a separate '
                                        'submission). This occurred in the time since your validations were completed. '
                                        'To prevent duplicate records, this submission must be revalidated in order to '
                                        'publish.',
                                        StatusCode.CLIENT_ERROR)

            total_count = sess.query(DetachedAwardFinancialAssistance). \
                filter_by(is_valid=True, submission_id=submission_id).count()
            log_derivation('Starting derivations for FABS submission (total count: {})'.format(total_count),
                           submission_id)

            # Insert all non-error, non-delete rows into published table
            column_list = [col.key for col in DetachedAwardFinancialAssistance.__table__.columns]
            remove_cols = ['created_at', 'updated_at', 'detached_award_financial_assistance_id', 'job_id', 'row_number',
                           'is_valid']
            for remove_col in remove_cols:
                column_list.remove(remove_col)
            detached_col_string = ", ".join(column_list)

            column_list = [col.key for col in PublishedAwardFinancialAssistance.__table__.columns]
            remove_cols = ['created_at', 'updated_at', 'modified_at', 'is_active',
                           'published_award_financial_assistance_id']
            for remove_col in remove_cols:
                column_list.remove(remove_col)
            published_col_string = ", ".join(column_list)

            log_derivation('Beginning transfer of publishable records to temp table', submission_id)
            create_table_sql = """
                CREATE TEMP TABLE tmp_fabs_{submission_id}
                ON COMMIT DROP
                AS
                    SELECT {cols}
                    FROM published_award_financial_assistance
                    WHERE false;

                ALTER TABLE tmp_fabs_{submission_id} ADD COLUMN published_award_financial_assistance_id
                    SERIAL PRIMARY KEY;
            """.format(submission_id=submission_id, cols=published_col_string)
            sess.execute(create_table_sql)

            create_indexes_sql = """
                CREATE INDEX ix_tmp_fabs_{submission_id}_funding_office_code_upper ON
                    tmp_fabs_{submission_id} (upper(funding_office_code));
                CREATE INDEX ix_tmp_fabs_{submission_id}_awarding_office_code_upper ON
                    tmp_fabs_{submission_id} (upper(awarding_office_code));
                CREATE INDEX ix_tmp_fabs_{submission_id}_business_funds_indicator_upper ON
                    tmp_fabs_{submission_id} (upper(business_funds_indicator));
                CREATE INDEX ix_tmp_fabs_{submission_id}_le_country_code_upper ON
                    tmp_fabs_{submission_id} (upper(legal_entity_country_code));
                CREATE INDEX ix_tmp_fabs_{submission_id}_le_state_code_upper ON
                    tmp_fabs_{submission_id} (upper(legal_entity_state_code));
                CREATE INDEX ix_tmp_fabs_{submission_id}_le_city_name_upper ON
                    tmp_fabs_{submission_id} (upper(legal_entity_city_name));
                CREATE INDEX ix_tmp_fabs_{submission_id}_business_types ON tmp_fabs_{submission_id} (business_types);
                CREATE INDEX ix_tmp_fabs_{submission_id}_award_mod ON
                    tmp_fabs_{submission_id} (award_modification_amendme);
                CREATE INDEX ix_tmp_fabs_{submission_id}_le_zip_last4 ON
                    tmp_fabs_{submission_id} (legal_entity_zip_last4);
                CREATE INDEX ix_tmp_fabs_{submission_id}_le_zip5 ON tmp_fabs_{submission_id} (legal_entity_zip5);
                CREATE INDEX ix_tmp_fabs_{submission_id}_le_state_code ON
                    tmp_fabs_{submission_id} (legal_entity_state_code);
                CREATE INDEX ix_tmp_fabs_{submission_id}_le_county_code ON
                    tmp_fabs_{submission_id} (legal_entity_county_code);
                CREATE INDEX ix_tmp_fabs_{submission_id}_le_congressional ON
                    tmp_fabs_{submission_id} (legal_entity_congressional);
                CREATE INDEX ix_tmp_fabs_{submission_id}_ppop_state_code ON
                    tmp_fabs_{submission_id} (place_of_perfor_state_code);
                CREATE INDEX ix_tmp_fabs_{submission_id}_ppop_zip5 ON
                    tmp_fabs_{submission_id} (place_of_performance_zip5);
                CREATE INDEX ix_tmp_fabs_{submission_id}_ppop_county_code ON
                    tmp_fabs_{submission_id} (place_of_perform_county_co);
                CREATE INDEX ix_tmp_fabs_{submission_id}_ppop_zip_last4 ON
                    tmp_fabs_{submission_id} (place_of_perform_zip_last4);

                CREATE INDEX ix_tmp_fabs_{submission_id}_uri_awarding_sub_tier_upper ON
                    tmp_fabs_{submission_id} (UPPER(uri), UPPER(awarding_sub_tier_agency_c));
                CREATE INDEX ix_tmp_fabs_{submission_id}_fain_awarding_sub_tier_upper ON
                    tmp_fabs_{submission_id} (UPPER(fain), UPPER(awarding_sub_tier_agency_c));

                CREATE INDEX ix_tmp_fabs_{submission_id}_awarding_sub_tier_upper ON
                    tmp_fabs_{submission_id} (upper(awarding_sub_tier_agency_c));
                CREATE INDEX ix_tmp_fabs_{submission_id}_uri_upper ON
                    tmp_fabs_{submission_id} (upper(uri));
                CREATE INDEX ix_tmp_fabs_{submission_id}_fain_upper ON
                    tmp_fabs_{submission_id} (upper(fain));
                CREATE INDEX ix_tmp_fabs_{submission_id}_cdi_upper ON
                    tmp_fabs_{submission_id} (upper(correction_delete_indicatr));
                CREATE INDEX ix_tmp_fabs_{submission_id}_cast_action_date_as_date ON
                    tmp_fabs_{submission_id} (cast_as_date(action_date));
                CREATE INDEX ix_tmp_fabs_{submission_id}_record_type ON
                    tmp_fabs_{submission_id} (record_type);
                CREATE INDEX ix_tmp_fabs_{submission_id}_ppop_congr ON
                    tmp_fabs_{submission_id} (place_of_performance_congr);
                CREATE INDEX ix_tmp_fabs_{submission_id}_ppop_country_upper ON
                    tmp_fabs_{submission_id} (UPPER(place_of_perform_country_c));
                CREATE INDEX ix_tmp_fabs_{submission_id}_funding_sub_tier_upper ON
                    tmp_fabs_{submission_id} (UPPER(funding_sub_tier_agency_co));
                CREATE INDEX ix_tmp_fabs_{submission_id}_cfda_num ON
                    tmp_fabs_{submission_id} (cfda_number);
                CREATE INDEX ix_tmp_fabs_{submission_id}_awardee_unique ON
                    tmp_fabs_{submission_id} (awardee_or_recipient_uniqu);
                CREATE INDEX ix_tmp_fabs_{submission_id}_assistance_type_upper ON
                    tmp_fabs_{submission_id} (upper(assistance_type));
                CREATE INDEX ix_tmp_fabs_{submission_id}_action_type_upper ON
                    tmp_fabs_{submission_id} (upper(action_type));
                CREATE INDEX ix_tmp_fabs_{submission_id}_uei_upper ON
                    tmp_fabs_{submission_id} (upper(uei));
            """.format(submission_id=submission_id)
            sess.execute(create_indexes_sql)

            insert_query = """
                INSERT INTO tmp_fabs_{submission_id} ({cols})
                SELECT {cols}
                FROM detached_award_financial_assistance AS dafa
                WHERE dafa.submission_id = {submission_id}
                    AND dafa.is_valid IS TRUE
                    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
            """
            sess.execute(insert_query.format(cols=detached_col_string, submission_id=submission_id))
            log_derivation('Completed transfer of publishable records to temp table', submission_id)

            fabs_start = datetime.now()
            log_derivation('Beginning main FABS derivations', submission_id)
            fabs_derivations(sess, submission_id)
            log_derivation('Completed main FABS derivations', submission_id, fabs_start)

            log_derivation('Beginning transfer of records from temp table to published table', submission_id)

            # Inserting non-delete records
            insert_query = """
                INSERT INTO published_award_financial_assistance (created_at, updated_at, {cols}, modified_at,
                                                                  is_active)
                SELECT NOW() AS created_at, NOW() AS updated_at, {cols}, NOW() AS modified_at, TRUE AS is_active
                FROM tmp_fabs_{submission_id} AS tmp_fabs;
            """
            sess.execute(insert_query.format(cols=published_col_string, submission_id=submission_id))

            # Inserting delete records, we didn't have to process these
            insert_query = """
                INSERT INTO published_award_financial_assistance (created_at, updated_at, {cols}, modified_at)
                SELECT NOW() AS created_at, NOW() AS updated_at, {cols}, NOW() AS modified_at
                FROM detached_award_financial_assistance AS dafa
                WHERE dafa.submission_id = {submission_id}
                    AND dafa.is_valid IS TRUE
                    AND UPPER(COALESCE(correction_delete_indicatr, '')) = 'D';
            """
            sess.execute(insert_query.format(cols=detached_col_string, submission_id=submission_id))

            log_derivation('Completed transfer of records from temp table to published table', submission_id)

            log_derivation('Beginning uncaching old files and deactivating old records', submission_id)
            # Deactivate all old records that have been updated with this submission
            deactivate_query = """
                WITH new_record_keys AS
                    (SELECT UPPER(afa_generated_unique) AS afa_generated_unique,
                        modified_at
                    FROM published_award_financial_assistance
                    WHERE submission_id={submission_id}
                        AND UPPER(correction_delete_indicatr) IN ('C', 'D'))
                UPDATE published_award_financial_assistance AS pafa
                SET is_active = FALSE,
                    updated_at = nrk.modified_at
                FROM new_record_keys AS nrk
                WHERE COALESCE(pafa.submission_id, 0) <> {submission_id}
                    AND UPPER(pafa.afa_generated_unique) = nrk.afa_generated_unique
                    AND is_active IS TRUE;
            """
            sess.execute(deactivate_query.format(submission_id=submission_id))

            # Uncache related files
            # Awarding agencies
            uncache_query = """
                WITH affected_agencies AS
                    (SELECT DISTINCT awarding_agency_code
                    FROM published_award_financial_assistance
                    WHERE submission_id={submission_id})
                UPDATE file_generation
                SET is_cached_file = FALSE
                FROM affected_agencies
                WHERE awarding_agency_code = agency_code
                    AND is_cached_file IS TRUE
                    AND file_type = 'D2';
            """
            sess.execute(uncache_query.format(submission_id=submission_id))

            # Funding agencies
            uncache_query = """
                WITH affected_agencies AS
                    (SELECT DISTINCT funding_agency_code
                    FROM published_award_financial_assistance
                    WHERE submission_id={submission_id})
                UPDATE file_generation
                SET is_cached_file = FALSE
                FROM affected_agencies
                WHERE funding_agency_code = agency_code
                    AND is_cached_file IS TRUE
                    AND file_type = 'D2';
            """
            sess.execute(uncache_query.format(submission_id=submission_id))
            log_derivation('Completed uncaching old files and deactivating old records', submission_id)

            sess.commit()
        except Exception as e:
            log_message = {
                'message': 'An error occurred while publishing a FABS submission',
                'message_type': 'BrokerError',
                'error_message': str(e),
                'submission_id': submission_id
            }
            logger.error(log_message)

            # rollback the changes if there are any errors. We want to submit everything together
            sess.rollback()

            sess.query(Submission).filter_by(submission_id=submission_id). \
                update({'publish_status_id': PUBLISH_STATUS_DICT['unpublished'], 'updated_at': datetime.utcnow()},
                       synchronize_session=False)
            sess.commit()

            # we want to return response exceptions in such a way that we can see the message, not catching it
            # separately because we still want to rollback the changes and set the status to unpublished
            if type(e) == ResponseException:
                return JsonResponse.error(e, e.status)

            return JsonResponse.error(e, StatusCode.INTERNAL_ERROR)
        log_derivation('Completed derivations for FABS submission', submission_id)

        sess.query(Submission).filter_by(submission_id=submission_id).\
            update({'publish_status_id': PUBLISH_STATUS_DICT['published'], 'publishing_user_id': g.user.user_id,
                    'updated_at': datetime.utcnow()}, synchronize_session=False)

        # create the publish_history and certify_history entries
        publish_history = PublishHistory(created_at=datetime.utcnow(), user_id=g.user.user_id,
                                         submission_id=submission_id)
        certify_history = CertifyHistory(created_at=datetime.utcnow(), user_id=g.user.user_id,
                                         submission_id=submission_id)
        sess.add_all([publish_history, certify_history])
        sess.commit()

        # get the publish_history entry including the PK
        publish_history = sess.query(PublishHistory).filter_by(submission_id=submission_id).\
            order_by(PublishHistory.created_at.desc()).first()

        # generate the published rows file and move all files
        # (locally we don't move but we still need to populate the published_files_history table)
        FileHandler.move_published_files(FileHandler, submission, publish_history, certify_history.certify_history_id,
                                         g.is_local)

        response_dict = {'submission_id': submission_id}
        return JsonResponse.create(StatusCode.OK, response_dict)

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
            except Exception:
                return JsonResponse.error(Exception('{} parameter must be a file in binary form'.format(file_type)),
                                          StatusCode.CLIENT_ERROR)
            if file_name:
                if not self.is_local:
                    upload_name = '{}/{}'.format(
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
            if 'submission_id' not in file_type:
                job_dict[file_type + '_id'] = file_job_dict[file_type]
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
        if submission.publish_status_id in (PUBLISH_STATUS_DICT['publishing'], PUBLISH_STATUS_DICT['reverting']):
            return JsonResponse.error(ValueError('Submission is certifying or reverting'), StatusCode.CLIENT_ERROR)

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
            if job.job_status_id == JOB_STATUS_DICT['running']:
                return JsonResponse.error(ValueError('Submission is still uploading or validating'),
                                          StatusCode.CLIENT_ERROR)

            if job.job_type_id == JOB_TYPE_DICT['file_upload'] and \
               job.file_type_id in [FILE_TYPE_DICT['award'], FILE_TYPE_DICT['award_procurement']]:
                # file generation handled on backend, mark as ready
                job.job_status_id = JOB_STATUS_DICT['ready']

                # forcibly uncache any related D file requests
                file_gen = sess.query(FileGeneration).filter_by(file_generation_id=job.file_generation_id).one_or_none()
                if file_gen:
                    file_gen.is_cached_file = False
            else:
                # these are dependent on file D2 validation
                job.job_status_id = JOB_STATUS_DICT['waiting']
            job.error_message = None

        # update upload jobs to "running" for files A, B, and C for DABS submissions or for the upload job in FABS
        upload_jobs = [job for job in jobs if job.job_type_id in [JOB_TYPE_DICT['file_upload']]
                       and job.file_type_id in initial_file_types]

        for job in upload_jobs:
            job.job_status_id = JOB_STATUS_DICT['running']
        sess.commit()

        # call finalize job for the upload jobs for files A, B, and C for DABS submissions and the only job for FABS,
        # which will kick off the rest of the process for DABS and indicate to the user that the validations are done
        # for FABS
        for job in upload_jobs:
            FileHandler.finalize(job.job_id)

        return JsonResponse.create(StatusCode.OK, {'message': 'Success'})

    def move_published_files(self, submission, publish_history, certify_history_id, is_local):
        """ Copy all files within the published submission to the correct published files bucket/directory. FABS
            submissions also create a file containing all the published rows

            Args:
                submission: submission for which to move the files
                publish_history: a PublishHistory object to use for timestamps and to update once the files are moved
                certify_history_id: the ID of a CertifyHistory object to update once the files are moved
                is_local: a boolean indicating whether the application is running locally or not
        """
        try:
            self.s3manager
        except AttributeError:
            self.s3manager = S3Handler()

        sess = GlobalDB.db().session
        submission_id = submission.submission_id
        log_data = {
            'message': 'Starting move_published_files',
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
        possible_warning_files = [FILE_TYPE_DICT['appropriations'], FILE_TYPE_DICT['program_activity'],
                                  FILE_TYPE_DICT['award_financial']]

        # set the route within the bucket
        if submission.d2_submission:
            created_at_date = publish_history.created_at
            route_vars = ['FABS', agency_code, created_at_date.year, '{:02d}'.format(created_at_date.month)]
        else:
            time_period = 'P{}'.format(str(submission.reporting_fiscal_period).zfill(2)) \
                if not submission.is_quarter_format else 'Q{}'.format(submission.reporting_fiscal_period // 3)
            route_vars = [agency_code, submission.reporting_fiscal_year, time_period,
                          publish_history.publish_history_id]
        new_route = '/'.join([str(var) for var in route_vars]) + '/'

        for job in jobs:
            log_data['job_id'] = job.job_id

            # non-local instances create a new path, local instances just use the existing one
            if not is_local:
                old_path_sections = job.filename.split('/')
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
                                             original_path='errors/' + warning_file_name, new_path=warning_file)
                else:
                    warning_file = CONFIG_SERVICES['error_report_path'] + report_file_name(submission_id, True,
                                                                                           job.file_type.name)

            comment = None
            if submission.d2_submission:
                # FABS published submission, create the FABS published rows file
                log_data['message'] = 'Generating published FABS file from publishable rows'
                logger.info(log_data)
                new_path = create_fabs_published_file(sess, submission_id, new_route)
            else:
                # DABS published submission
                # get the comment relating to the file
                comment = sess.query(Comment).\
                    filter_by(submission_id=submission_id, file_type_id=job.file_type_id).one_or_none()
                if comment:
                    comment = comment.comment

                # only actually move the files if it's not a local submission
                if not is_local:
                    self.s3manager.copy_file(original_bucket=original_bucket, new_bucket=new_bucket,
                                             original_path=job.filename, new_path=new_path)

            # create the published_files_history for this file
            file_history = PublishedFilesHistory(publish_history_id=publish_history.publish_history_id,
                                                 certify_history_id=certify_history_id,
                                                 submission_id=submission_id, file_type_id=job.file_type_id,
                                                 filename=new_path, comment=comment,
                                                 warning_filename=warning_file)
            sess.add(file_history)

        # FABS submissions don't have cross-file validations or comments
        if not submission.d2_submission:
            # Adding cross-file warnings
            cross_list = {'B': 'A', 'C': 'B', 'D1': 'C', 'D2': 'C'}
            for key, value in cross_list.items():
                first_file = FILE_TYPE_DICT_LETTER_NAME[value]
                second_file = FILE_TYPE_DICT_LETTER_NAME[key]

                # create warning file path
                if not is_local:
                    warning_file_name = report_file_name(submission_id, True, first_file, second_file)
                    warning_file = new_route + warning_file_name

                    # move the file if we aren't local
                    self.s3manager.copy_file(original_bucket=original_bucket, new_bucket=new_bucket,
                                             original_path='errors/' + warning_file_name, new_path=warning_file)
                else:
                    warning_file = CONFIG_SERVICES['error_report_path'] + report_file_name(submission_id, True,
                                                                                           first_file, second_file)

                # add published history
                file_history = PublishedFilesHistory(publish_history_id=publish_history.publish_history_id,
                                                     certify_history_id=certify_history_id,
                                                     submission_id=submission_id, filename=None, file_type_id=None,
                                                     comment=None, warning_filename=warning_file)
                sess.add(file_history)

            # Only move the file if we have any published comments
            num_cert_comments = sess.query(CertifiedComment).filter_by(submission_id=submission_id).count()
            if num_cert_comments > 0:
                filename = 'submission_{}_comments.csv'.format(str(submission_id))
                if not is_local:
                    old_path = '{}/{}'.format(str(submission.submission_id), filename)
                    new_path = new_route + filename
                    # Copy the file if it's a non-local submission
                    self.s3manager.copy_file(original_bucket=original_bucket, new_bucket=new_bucket,
                                             original_path=old_path, new_path=new_path)
                else:
                    new_path = ''.join([CONFIG_BROKER['broker_files'], filename])
                file_history = PublishedFilesHistory(publish_history_id=publish_history.publish_history_id,
                                                     certify_history_id=certify_history_id,
                                                     submission_id=submission_id, filename=new_path, file_type_id=None,
                                                     comment=None, warning_filename=None)
                sess.add(file_history)
        sess.commit()

        log_data['message'] = 'Completed move_published_files'
        logger.debug(log_data)

    def revert_published_error_files(self, sess, publish_history_id):
        """ Copy warning files (non-locally) back to the errors folder and revert error files to just headers for a
            submission that is being reverted to published/certified status

            Args:
                sess: the database connection
                publish_history_id: the ID of the PublishHistory object that represents the latest publication
        """
        warning_files = sess.query(PublishedFilesHistory.warning_filename). \
            filter(PublishedFilesHistory.publish_history_id == publish_history_id,
                   PublishedFilesHistory.warning_filename.isnot(None)).all()
        for warning in warning_files:
            warning = warning.warning_filename
            # Getting headers and file names
            if 'cross' in warning:
                error = warning.replace('_warning_', '_')
                headers = ValidationManager.cross_file_report_headers
            else:
                error = warning.replace('warning', 'error')
                headers = ValidationManager.report_headers

            # Moving/clearing files
            if not self.is_local:
                s3_resource = boto3.resource('s3', region_name=CONFIG_BROKER['aws_region'])
                submission_bucket = CONFIG_BROKER['aws_bucket']
                certified_bucket = CONFIG_BROKER['certified_bucket']

                error_file_name = os.path.basename(error)
                warning_file_name = os.path.basename(warning)
                error_file_path = ''.join([CONFIG_SERVICES['error_report_path'], error_file_name])

                # Create clean error file
                with open(error_file_path, 'w', newline='') as error_file:
                    error_csv = csv.writer(error_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
                    error_csv.writerow(headers)
                error_file.close()

                # Write error file
                with open(error_file_path, 'rb') as csv_file:
                    s3_resource.Object(submission_bucket, 'errors/' + error_file_name).put(Body=csv_file)
                csv_file.close()
                os.remove(error_file_path)

                # Copy warning file back over
                S3Handler.copy_file(original_bucket=certified_bucket, new_bucket=submission_bucket,
                                    original_path=warning, new_path='errors/' + warning_file_name)
            else:
                with open(error, 'w', newline='') as error_file:
                    error_csv = csv.writer(error_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
                    error_csv.writerow(headers)
                error_file.close()


def get_submission_comments(submission):
    """ Fetch comments for this submission, indexed by file letter

        Args:
            submission: the submission to gather comments for

        Returns:
            JsonResponse object with the contents of the comments in a key/value pair of letter/comments
    """
    sess = GlobalDB.db().session
    result = {letter: '' for letter in FILE_TYPE_DICT_LETTER.values() if letter != 'FABS'}
    result['submission_comment'] = ''
    comments = sess.query(Comment).filter_by(submission_id=submission.submission_id)
    for comment in comments:
        comment_type = FILE_TYPE_DICT_LETTER.get(comment.file_type_id, 'submission_comment')
        result[comment_type] = comment.comment
    return JsonResponse.create(StatusCode.OK, result)


def update_submission_comments(submission, comment_request, is_local):
    """ Clear existing comments and replace them with the provided set.

        Args:
            submission: submission to update the comments for
            comment_request: the contents of the request from the API
            is_local: a boolean indicating whether the application is running locally or not
    """
    # If the submission has been published, set its status to updated when new comments are made.
    if submission.publish_status_id == PUBLISH_STATUS_DICT['published']:
        submission.publish_status_id = PUBLISH_STATUS_DICT['updated']

    json = comment_request or {}
    # clean input
    comments_json = {key.upper(): value.strip() for key, value in json.items()
                     if isinstance(value, str) and value.strip()}

    sess = GlobalDB.db().session
    # Delete old comments, fetch just in case
    sess.query(Comment).filter_by(submission_id=submission.submission_id).delete(synchronize_session='fetch')

    comments = []
    for file_type_id, letter in FILE_TYPE_DICT_LETTER.items():
        if letter in comments_json and letter != 'FABS':
            comments.append(Comment(
                submission_id=submission.submission_id,
                file_type_id=file_type_id,
                comment=comments_json[letter]
            ))
    submission_comment = comments_json.get('SUBMISSION_COMMENT')
    if submission_comment:
        comments.append(Comment(
            submission_id=submission.submission_id,
            file_type_id=None,
            comment=submission_comment
        ))
    sess.add_all(comments)
    sess.commit()

    # Preparing for the comments file
    filename = 'submission_{}_comments.csv'.format(submission.submission_id)
    local_file = ''.join([CONFIG_BROKER['broker_files'], filename])
    file_path = local_file if is_local else '{}/{}'.format(str(submission.submission_id), filename)
    headers = ['Comment Type', 'Comment']

    # Generate a file containing all the comments for a given submission
    comment_query = sess.query(
        case([
            (FileType.name.isnot(None), FileType.name),
            (FileType.name.is_(None), 'Submission Comment')
        ]), Comment.comment).\
        outerjoin(FileType, Comment.file_type_id == FileType.file_type_id).\
        filter(Comment.submission_id == submission.submission_id)

    # Generate the file locally, then place in S3
    write_stream_query(sess, comment_query, local_file, file_path, is_local, header=headers)

    return JsonResponse.create(StatusCode.OK, {})


def get_comments_file(submission, is_local):
    """ Retrieve the comments file for a specific submission.

        Args:
            submission: the submission to get the comments file for
            is_local: a boolean indicating whether the application is running locally or not

        Returns:
            A JsonResponse containing the url to the file if one exists, JsonResponse error containing the details of
            the error if something went wrong
    """

    sess = GlobalDB.db().session
    num_comments = sess.query(Comment).filter_by(submission_id=submission.submission_id).count()
    # if we have at least one comment, we have a file to return
    if num_comments > 0:
        filename = 'submission_{}_comments.csv'.format(submission.submission_id)
        if is_local:
            # when local, can just grab the path
            url = os.path.join(CONFIG_BROKER['broker_files'], filename)
        else:
            url = S3Handler().get_signed_url(str(submission.submission_id), filename,
                                             url_mapping=CONFIG_BROKER['submission_bucket_mapping'],
                                             method='get_object')
        return JsonResponse.create(StatusCode.OK, {'url': url})
    return JsonResponse.error(ValueError('This submission does not have any comments associated with it'),
                              StatusCode.CLIENT_ERROR)


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
    local_filename = ''.join([CONFIG_BROKER['broker_files'], timestamped_name])
    upload_name = ''.join([new_route, timestamped_name])

    # write file and stream to S3
    fabs_query = published_fabs_query({'sess': sess, 'submission_id': submission_id})
    headers = [val[0] for key, val in fileD2.mapping.items()]
    write_stream_query(sess, fabs_query, local_filename, upload_name, g.is_local, header=headers, is_certified=True)
    return local_filename if g.is_local else upload_name


def published_fabs_query(data_utils):
    """ Get the data from the published FABS table to write to the file with

        Args:
            data_utils: A dictionary of utils that are needed for the query being made, in this case including the
                session object and the submission ID

        Returns:
            A list of published FABS rows.
    """
    return fileD2.query_published_fabs_data(data_utils['sess'], data_utils['submission_id'])


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
        dict_key = 'cross' if not job.file_type else job.file_type_name

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
    validation = None
    upload_status = ''
    validation_status = ''
    upload_em = ''
    validation_em = ''
    ready_statuses = ['ready', 'waiting']
    for job in jobs:
        if job['job_type'] == JOB_TYPE_DICT['file_upload']:
            upload_status = JOB_STATUS_DICT_ID[job['job_status']]
            upload_em = job['error_message']
        else:
            validation = job
            validation_status = JOB_STATUS_DICT_ID[job['job_status']]
            validation_em = job['error_message']

    # checking for failures
    if upload_status == 'invalid' or upload_status == 'failed' or validation_status == 'failed':
        response_content['status'] = 'failed'
        response_content['has_errors'] = True
        response_content['message'] = upload_em or validation_em or ''
        return response_content

    # If only validation_status is invalid then that means it's a header error or something similar, technically still
    # a finished job and needs to be able to display that error on the frontend
    if validation_status == 'invalid':
        response_content['status'] = 'finished'
        response_content['has_errors'] = True
        response_content['message'] = upload_em or validation_em or ''
        return response_content

    # If upload job exists and hasn't started or if it doesn't exist and validation job hasn't started,
    # it should just be ready
    if upload_status in ready_statuses or (upload_status == '' and validation_status in ready_statuses):
        return response_content

    # If the upload job is running, status is uploading
    if upload_status == 'running':
        response_content['status'] = 'uploading'
        return response_content

    # If the validation job is running, status is running. If upload is finished and validation is ready, it should
    # be started soon and is technically running
    if validation_status == 'running' or (upload_status == 'finished' and validation_status in ready_statuses):
        response_content['status'] = 'running'
        return response_content

    # If both jobs are finished, figure out if there are errors
    response_content['status'] = 'finished'
    response_content['has_errors'] = validation is not None and validation['errors'] > 0
    response_content['has_warnings'] = validation is not None and validation['warnings'] > 0
    return response_content


def add_list_submission_filters(query, filters, submission_updated_view):
    """ Add provided filters to the list_submission query

        Args:
            query: already existing query to add to
            filters: provided filters
            submission_updated_view: The view containing the max updated_at dates for all submissions

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
            try:
                sub_list = [int(sub_id) for sub_id in sub_list]
            except ValueError:
                raise ResponseException('All submission_ids must be valid submission IDs', StatusCode.CLIENT_ERROR)
            query = query.filter(Submission.submission_id.in_(sub_list))
        elif sub_list:
            raise ResponseException('submission_ids filter must be null or an array', StatusCode.CLIENT_ERROR)
    # Date range filter
    if 'last_modified_range' in filters:
        mod_dates = filters['last_modified_range']
        # last_modified_range must be a dict
        if mod_dates and isinstance(mod_dates, dict):
            start_date = mod_dates.get('start_date')
            end_date = mod_dates.get('end_date')

            # Make sure that, if it has content, at least start_date or end_date is part of this filter
            if not start_date and not end_date:
                raise ResponseException('At least start_date or end_date must be provided when using '
                                        'last_modified_range filter', StatusCode.CLIENT_ERROR)

            # Start and end dates, when provided must be in the format MM/DD/YYYY format
            if (start_date and not StringCleaner.is_date(start_date)) or\
                    (end_date and not StringCleaner.is_date(end_date)):
                raise ResponseException('Start or end date cannot be parsed into a date of format MM/DD/YYYY',
                                        StatusCode.CLIENT_ERROR)
            # Make sure start date is not greater than end date when both are provided (checking for >= because we add a
            # day)
            start_date = datetime.strptime(start_date, '%m/%d/%Y') if start_date else None
            end_date = datetime.strptime(end_date, '%m/%d/%Y') + timedelta(days=1) if end_date else None
            if start_date and end_date and start_date >= end_date:
                raise ResponseException('Last modified start date cannot be greater than the end date',
                                        StatusCode.CLIENT_ERROR)
            if start_date:
                query = query.filter(submission_updated_view.updated_at >= start_date)
            if end_date:
                query = query.filter(submission_updated_view.updated_at < end_date)
        elif mod_dates:
            raise ResponseException('last_modified_range filter must be null or an object', StatusCode.CLIENT_ERROR)
    # Agency code filter
    if 'agency_codes' in filters:
        agency_list = filters['agency_codes']
        if agency_list and isinstance(agency_list, list):
            query = agency_filter(sess, query, CGAC, FREC, agency_list)
        elif agency_list:
            raise ResponseException('agency_codes filter must be null or an array', StatusCode.CLIENT_ERROR)
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
            raise ResponseException('file_names filter must be null or an array', StatusCode.CLIENT_ERROR)
    # User ID filter
    if 'user_ids' in filters:
        user_list = filters['user_ids']
        if user_list and isinstance(user_list, list):
            try:
                user_list = [int(user_id) for user_id in user_list]
            except ValueError:
                raise ResponseException('All user_ids must be valid user IDs', StatusCode.CLIENT_ERROR)
            query = query.filter(Submission.user_id.in_(user_list))
        elif user_list:
            raise ResponseException('user_ids filter must be null or an array', StatusCode.CLIENT_ERROR)
    # Test submission filter
    if 'submission_type' in filters:
        submission_type = filters['submission_type']
        if str(submission_type).upper() in ['TEST', 'CERTIFIABLE']:
            query = query.filter(Submission.test_submission.is_(submission_type.upper() == 'TEST'))
        else:
            raise ResponseException('submission_type filter must be "test" or "certifiable"', StatusCode.CLIENT_ERROR)
    return query


def list_submissions(page, limit, published, sort='modified', order='desc', is_fabs=False, filters=None):
    """ List submission based on current page and amount to display. If provided, filter based on publication status

        Args:
            page: page number to use in getting the list
            limit: the number of entries per page
            published: string indicating whether to display only published, only unpublished, or both for submissions
            sort: the column to order on
            order: order ascending or descending
            is_fabs: boolean indicating whether it is a DABS or FABS submission (True if FABS)
            filters: an object containing the filters provided by the user

        Returns:
            Limited list of submissions and the total number of submissions the user has access to
    """
    sess = GlobalDB.db().session
    submission_updated_view = SubmissionUpdatedView()
    offset = limit * (page - 1)
    publishing_user = aliased(User)

    # List of all the columns to gather
    submission_columns = [Submission.submission_id, Submission.cgac_code, Submission.frec_code, Submission.user_id,
                          Submission.publish_status_id, Submission.d2_submission, Submission.number_of_warnings,
                          Submission.number_of_errors, Submission.updated_at, Submission.reporting_start_date,
                          Submission.reporting_end_date, Submission.publishing_user_id,
                          Submission.reporting_fiscal_year, Submission.reporting_fiscal_period,
                          Submission.is_quarter_format, Submission.published_submission_ids, Submission.certified,
                          Submission.test_submission]
    cgac_columns = [CGAC.cgac_code, CGAC.agency_name.label('cgac_agency_name')]
    frec_columns = [FREC.frec_code, FREC.agency_name.label('frec_agency_name')]
    user_columns = [User.user_id, User.name, publishing_user.user_id.label('publishing_user_id'),
                    publishing_user.name.label('publishing_user_name')]
    view_columns = [submission_updated_view.submission_id, submission_updated_view.updated_at.label('updated_at')]
    max_pub = sess.query(PublishHistory.submission_id, func.max(PublishHistory.created_at).label('max_date')).\
        group_by(PublishHistory.submission_id).cte('max_pub')
    max_cert = sess.query(CertifyHistory.submission_id, func.max(CertifyHistory.created_at).label('max_date')). \
        group_by(CertifyHistory.submission_id).cte('max_cert')
    pub_query = sess.query(max_pub.c.submission_id,
                           case([(func.coalesce(max_cert.c.max_date, '1/1/1973') > max_pub.c.max_date,
                                  max_cert.c.max_date)], else_=max_pub.c.max_date).label('last_pub_or_cert')).\
        outerjoin(max_cert, max_pub.c.submission_id == max_cert.c.submission_id).cte('pub_query')

    columns_to_query = (submission_columns + cgac_columns + frec_columns + user_columns + view_columns
                        + [pub_query.c.last_pub_or_cert])

    # Base query that is shared among all submission lists
    query = sess.query(*columns_to_query).\
        outerjoin(User, Submission.user_id == User.user_id).\
        outerjoin(publishing_user, Submission.publishing_user_id == publishing_user.user_id).\
        outerjoin(CGAC, Submission.cgac_code == CGAC.cgac_code).\
        outerjoin(FREC, Submission.frec_code == FREC.frec_code).\
        outerjoin(submission_updated_view.table, submission_updated_view.submission_id == Submission.submission_id).\
        outerjoin(pub_query, Submission.submission_id == pub_query.c.submission_id).\
        filter(Submission.d2_submission.is_(is_fabs))
    min_mod_query = sess.query(func.min(submission_updated_view.updated_at).label('min_last_mod_date')). \
        join(Submission, submission_updated_view.submission_id == Submission.submission_id).\
        filter(Submission.d2_submission.is_(is_fabs))

    # Limit the data coming back to only what the given user is allowed to see
    query = permissions_filter(query)
    min_mod_query = permissions_filter(min_mod_query)

    # Determine what types of submissions (published/unpublished/both) to display
    if published != 'mixed':
        if published == 'true':
            query = query.filter(Submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished'])
            min_mod_query = min_mod_query.filter(Submission.publish_status_id != PUBLISH_STATUS_DICT['unpublished'])
        else:
            query = query.filter(Submission.publish_status_id == PUBLISH_STATUS_DICT['unpublished'])
            min_mod_query = min_mod_query.filter(Submission.publish_status_id == PUBLISH_STATUS_DICT['unpublished'])

    # Add additional filters where applicable
    if filters:
        try:
            query = add_list_submission_filters(query, filters, submission_updated_view)
        except (ResponseException, ValueError) as e:
            return JsonResponse.error(e, StatusCode.CLIENT_ERROR)

    # Determine what to order by, default to "modified"
    options = {
        'submission_id': {'model': Submission, 'col': 'submission_id'},
        'modified': {'model': submission_updated_view, 'col': 'updated_at'},
        'reporting_start': {'model': Submission, 'col': 'reporting_start_date'},
        'reporting_end': {'model': Submission, 'col': 'reporting_end_date'},
        'agency': {'model': CGAC, 'col': 'agency_name'},
        'submitted_by': {'model': User, 'col': 'name'},
        'last_pub_or_cert': {'model': pub_query.c, 'col': 'last_pub_or_cert'},
        'quarterly_submission': {'model': Submission, 'col': 'is_quarter_format'}
    }

    if not options.get(sort):
        sort = 'modified'

    sort_order = getattr(options[sort]['model'], options[sort]['col'])

    # Determine how to sort agencies using FREC or CGAC name
    if sort == 'agency':
        sort_order = case([
            (FREC.agency_name.isnot(None), FREC.agency_name),
            (CGAC.agency_name.isnot(None), CGAC.agency_name)
        ])

    # Set the sort order
    if order == 'desc':
        sort_order = sort_order.desc()

    query = query.order_by(sort_order)

    total_submissions = query.count()
    min_last_mod = min_mod_query.one()[0]

    query = query.slice(offset, offset + limit)

    return JsonResponse.create(StatusCode.OK, {
        'submissions': [serialize_submission(submission) for submission in query],
        'total': total_submissions,
        'min_last_modified': str(min_last_mod) if min_last_mod else None
    })


def process_history_list(history_list, list_type):
    """ Processes gathering a file history list for either publish or certify history.

        Args:
            history_list: the PublishHistory or CertifyHistory list to run through
            list_type: whether the list being processed is publish or certify

        Returns:
            The list with details of the file history
    """
    sess = GlobalDB.db().session
    processed_list = []

    for history in history_list:
        user = sess.query(User).filter_by(user_id=history.user_id).one()

        file_history_query = sess.query(PublishedFilesHistory)
        if list_type == 'certify':
            file_history_query = file_history_query.filter_by(certify_history_id=history.certify_history_id)
        else:
            file_history_query = file_history_query.filter_by(publish_history_id=history.publish_history_id)
        file_history = file_history_query.all()
        history_files = []

        for file in file_history:
            # if there's a filename, add it to the list
            if file.filename is not None:
                history_files.append({
                    'published_files_history_id': file.published_files_history_id,
                    'filename': file.filename.split('/')[-1],
                    'is_warning': False,
                    'comment': file.comment
                })

            # if there's a warning file, add it to the list
            if file.warning_filename is not None:
                history_files.append({
                    'published_files_history_id': file.published_files_history_id,
                    'filename': file.warning_filename.split('/')[-1],
                    'is_warning': True,
                    'comment': None
                })

        processed_list.append({
            '{}_date'.format(list_type): history.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            '{}ing_user'.format(list_type): {'name': user.name, 'user_id': history.user_id},
            '{}ed_files'.format(list_type if list_type == 'publish' else 'certifi'): history_files
        })

    return processed_list


def list_history(submission):
    """ List all publications and certifications for a single submission including the file history that goes with them.

        Args:
            submission: submission to get publictions and certifications for

        Returns:
            A JsonResponse containing a dictionary with the submission ID and a list of publications and certifications
            or a JsonResponse error containing details of what went wrong.
    """
    # TODO: Split this into published and certified lists, probably can leave it as one endpoint
    if submission.d2_submission:
        return JsonResponse.error(ValueError('FABS submissions do not have a publication history'),
                                  StatusCode.CLIENT_ERROR)

    sess = GlobalDB.db().session

    publish_history = sess.query(PublishHistory).filter_by(submission_id=submission.submission_id).\
        order_by(PublishHistory.created_at.desc()).all()
    certify_history = sess.query(CertifyHistory).filter_by(submission_id=submission.submission_id).\
        order_by(CertifyHistory.created_at.desc()).all()

    # We only have to check publish history because there can be no certify history without publish history
    if len(publish_history) == 0:
        return JsonResponse.error(ValueError('This submission has no publication history'), StatusCode.CLIENT_ERROR)

    # get the details for each of the publications/certifications
    publications = process_history_list(publish_history, 'publish')
    certifications = process_history_list(certify_history, 'certify')

    return JsonResponse.create(StatusCode.OK, {'submission_id': submission.submission_id,
                                               'publications': publications,
                                               'certifications': certifications})


def file_history_url(file_history_id, is_warning, is_local, submission=None):
    """ Get the signed URL for the specified historical file

        Args:
            file_history_id: the PublishedFilesHistory ID to get the file from
            is_warning: a boolean indicating if the file being retrieved is a warning or base file (True for warning)
            is_local: a boolean indicating if the application is being run locally (True for local)
            submission: submission to get the file history for

        Returns:
            A JsonResponse containing a dictionary with the url to the file or a JsonResponse error containing details
            of what went wrong.
    """
    sess = GlobalDB.db().session

    file_history = sess.query(PublishedFilesHistory).filter_by(published_files_history_id=file_history_id).one_or_none()

    if not file_history:
        return JsonResponse.error(ValueError('Invalid published_files_history_id'), StatusCode.CLIENT_ERROR)

    # TODO: Reassess if we really need to include the submission ID for this, is there a better way to check perms?
    if submission:
        if file_history.submission_id != submission.submission_id:
            return JsonResponse.error(ValueError('Requested published_files_history_id does not '
                                                 'match submission_id provided'), StatusCode.CLIENT_ERROR)

    if is_warning and not file_history.warning_filename:
        return JsonResponse.error(ValueError('History entry has no warning file'), StatusCode.CLIENT_ERROR)

    if not is_warning and not file_history.filename:
        return JsonResponse.error(ValueError('History entry has no related file'), StatusCode.CLIENT_ERROR)

    # locally, just return the filepath
    if is_local:
        if is_warning:
            url = file_history.warning_filename
        else:
            url = file_history.filename
    else:
        if is_warning:
            file_array = file_history.warning_filename.split('/')
        else:
            file_array = file_history.filename.split('/')

        # Remove the last part of the array before putting it back together so we can use our existing functions
        filename = file_array.pop()
        file_path = '/'.join(x for x in file_array)
        url = S3Handler().get_signed_url(file_path, filename, bucket_route=CONFIG_BROKER['certified_bucket'],
                                         url_mapping=CONFIG_BROKER['certified_bucket_mapping'],
                                         method='get_object')

    return JsonResponse.create(StatusCode.OK, {'url': url})


def list_published_files(sub_type, agency=None, year=None, period=None):
    """ List all the latest published files if all filters provided. Otherwise, provide the next filter options to be
        used for the Raw Files page.

        Args:
            type: must be dabs/fabs
            agency: the relevant agency code (cgac or frec)
            year: the relevant year
            period: the relevant period/month

        Returns:
            If all filters provided, list of names and published file ids
                {[name: 'file name', publish_files_history_id: 231}, ...]
            Otherwise, return the next available options for the next filter level
                Order: type -> agency -> year -> period
    """
    sess = GlobalDB.db().session

    # determine type of request
    if sub_type not in ['dabs', 'fabs']:
        raise ResponseException('Type must be either \'dabs\' or \'fabs\'')

    # figure out what to return based on the request
    filters_provided = []
    for filter_provided, level in [(sub_type, 'type'), (agency, 'agency'), (year, 'year'), (period, 'period')]:
        if filter_provided is not None:
            filters_provided.append(level)
        else:
            break

    # figure out the selects/filters based on the request
    selects = []
    filters = []
    order_by = []
    distinct = True
    published_files_month = extract('month', PublishedFilesHistory.created_at)
    published_files_year = extract('year', PublishedFilesHistory.created_at)
    if 'type' in filters_provided:
        selects = [
            case([
                (FREC.frec_code.isnot(None), FREC.frec_code),
                (CGAC.cgac_code.isnot(None), CGAC.cgac_code)
            ]).label('agency_code'),
            case([
                (FREC.agency_name.isnot(None), FREC.agency_name),
                (CGAC.agency_name.isnot(None), CGAC.agency_name)
            ]).label('agency_name')]
        filters += [Submission.d2_submission == (sub_type == 'fabs')]
        order_by = [selects[1]]
    if 'agency' in filters_provided:
        selects = [Submission.reporting_fiscal_year if sub_type == 'dabs'
                   else case([
                       (published_files_month >= 10, cast(published_files_year + 1, Integer)),
                       (published_files_month < 10, cast(published_files_year, Integer))
                   ]).label('reporting_fiscal_year')]
        # filters added after initial query construction
        order_by = [selects[0]]
    if 'year' in filters_provided:
        selects = [Submission.reporting_fiscal_period if sub_type == 'dabs'
                   else case([
                       (published_files_month >= 10, cast(published_files_month - 9, Integer)),
                       (published_files_month < 10, cast(published_files_month + 3, Integer)),
                   ]).label('reporting_fiscal_period')]
        filters += [Submission.reporting_fiscal_year == str(year) if sub_type == 'dabs'
                    else case([
                        (published_files_month >= 10, published_files_year + 1 == str(year)),
                        (published_files_month < 10, published_files_year == str(year))
                    ])]
        order_by = [selects[0]]
    if 'period' in filters_provided:
        distinct = False
        selects = [
            PublishedFilesHistory.published_files_history_id,
            PublishedFilesHistory.file_type_id,
            PublishedFilesHistory.filename,
            Submission.submission_id,
            case([
                (FREC.frec_code.isnot(None), FREC.frec_code),
                (CGAC.cgac_code.isnot(None), CGAC.cgac_code)
            ]).label('agency_code'),
            case([
                (FREC.agency_name.isnot(None), FREC.agency_name),
                (CGAC.agency_name.isnot(None), CGAC.agency_name)
            ]).label('agency_name'),
            Submission.is_quarter_format]
        filters += [Submission.reporting_fiscal_period == str(period) if sub_type == 'dabs'
                    else case([
                        (published_files_month >= 10, published_files_month - 9 == str(period)),
                        (published_files_month < 10, published_files_month + 3 == str(period))
                    ]),
                    PublishedFilesHistory.filename.isnot(None),
                    PublishedFilesHistory.file_type_id.isnot(None)]
        order_by = [Submission.submission_id, PublishedFilesHistory.file_type_id]

    # making sure we're only pulling the latest published per submission
    published_ids = sess. \
        query(func.max(PublishedFilesHistory.publish_history_id).label('max_pub_id')). \
        group_by(PublishedFilesHistory.submission_id).cte('published_ids')
    # put it all together
    query = sess.query(*selects). \
        select_from(PublishedFilesHistory). \
        join(Submission, PublishedFilesHistory.submission_id == Submission.submission_id). \
        join(published_ids, published_ids.c.max_pub_id == PublishedFilesHistory.publish_history_id).\
        outerjoin(CGAC, CGAC.cgac_code == Submission.cgac_code). \
        outerjoin(FREC, FREC.frec_code == Submission.frec_code). \
        filter(*filters). \
        order_by(*order_by)
    if distinct:
        query = query.distinct()
    # agency filter can only be provided after the base query's been built
    if 'agency' in filters_provided:
        query = agency_filter(sess, query, Submission, Submission, [agency])

    # present the results
    results = []
    if filters_provided[-1] == 'type':
        for result in query:
            results.append({'id': result.agency_code,
                            'label': '{} - {}'.format(result.agency_code, result.agency_name)})
    elif filters_provided[-1] == 'agency':
        for result in query:
            results.append({'id': result.reporting_fiscal_year, 'label': str(result.reporting_fiscal_year)})
    elif filters_provided[-1] == 'year':
        for result in query:
            if result.reporting_fiscal_period == 2 and sub_type == 'dabs':
                period = 'P01-P02'
            elif result.reporting_fiscal_period % 3 == 0 and sub_type == 'dabs':
                period = 'P{}/Q{}'.format(str(result.reporting_fiscal_period).zfill(2),
                                          int(result.reporting_fiscal_period / 3))
            else:
                period = 'P{}'.format(str(result.reporting_fiscal_period).zfill(2))
            results.append({'id': result.reporting_fiscal_period, 'label': period})
    else:
        comments_file_check = False
        for result in query:
            results.append({
                'id': result.published_files_history_id,
                'label': os.path.basename(result.filename),
                'filetype': FILE_TYPE_DICT_LETTER[result.file_type_id],
                'submission_id': result.submission_id
            })
            if sub_type == 'dabs' and not comments_file_check:
                agency_name = '{}_{}'.format(result.agency_code, result.agency_name)
                if result.is_quarter_format:
                    period = 'Q{}'.format(int(period / 3))
                else:
                    period = 'P{}'.format(str(period).zfill(2))
                filename = '{}-{}-{}-Agency_Comments.txt'.format(year, period, agency_name)
                agency_comments_url = os.path.join(CONFIG_BROKER['usas_public_submissions_url'], filename)

                try:
                    r = requests.head(agency_comments_url)
                    if r.status_code == requests.codes.ok:
                        results.append({
                            'id': None,
                            'label': filename,
                            'filetype': 'comments',
                            'submission_id': result.submission_id
                        })
                except Exception as e:
                    logger.exception(e)
                comments_file_check = True

    return JsonResponse.create(StatusCode.OK, results)


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
    time_period = get_time_period(submission)
    agency_name = submission.cgac_agency_name if submission.cgac_agency_name else submission.frec_agency_name
    expire_date = (submission.updated_at + relativedelta(months=6)).strftime('%Y-%m-%d')
    return {
        'submission_id': submission.submission_id,
        'last_modified': str(submission.updated_at),
        'expiration_date': expire_date if submission.test_submission else None,
        'status': status,
        'agency': agency_name if agency_name else 'N/A',
        'files': files,
        # @todo why are these a different format?
        'reporting_start_date': str(submission.reporting_start_date) if submission.reporting_start_date else None,
        'reporting_end_date': str(submission.reporting_end_date) if submission.reporting_end_date else None,
        'user': {'user_id': submission.user_id, 'name': submission.name if submission.name else 'No User'},
        'publishing_user': submission.publishing_user_name if submission.publishing_user_name else '',
        'publish_status': PUBLISH_STATUS_DICT_ID[submission.publish_status_id],
        'test_submission': submission.test_submission,
        'last_pub_or_cert': str(submission.last_pub_or_cert) if submission.last_pub_or_cert else '',
        'quarterly_submission': submission.is_quarter_format,
        'certified': submission.certified,
        'time_period': time_period
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
            return JsonResponse.error(ValueError('{} and {} is not a valid cross-pair.'.format(file_type, cross_type)),
                                      StatusCode.CLIENT_ERROR)

    # Get the url
    file_name = report_file_name(submission.submission_id, warning, file_type, cross_type)
    if CONFIG_BROKER['local']:
        url = os.path.join(CONFIG_SERVICES['error_report_path'], file_name)
    else:
        url = S3Handler().get_signed_url('errors', file_name,
                                         url_mapping=CONFIG_BROKER['submission_bucket_mapping'],
                                         method='get_object')
    return JsonResponse.create(StatusCode.OK, {'url': url})


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
        return JsonResponse.error(ValueError('Invalid file type for this submission'), StatusCode.CLIENT_ERROR)

    sess = GlobalDB.db().session
    file_job = sess.query(Job).filter(Job.submission_id == submission.submission_id,
                                      Job.file_type_id == FILE_TYPE_DICT_LETTER_ID[file_type],
                                      Job.job_type_id == JOB_TYPE_DICT['file_upload']).first()
    if not file_job.filename:
        return JsonResponse.error(ValueError('No file uploaded or generated for this type'), StatusCode.CLIENT_ERROR)

    split_name = file_job.filename.split('/')
    if CONFIG_BROKER['local']:
        # when local, can just grab the filename because it stores the entire path
        url = os.path.join(CONFIG_BROKER['broker_files'], split_name[-1])
    else:
        url = S3Handler().get_signed_url(split_name[0], split_name[1],
                                         url_mapping=CONFIG_BROKER['submission_bucket_mapping'], method='get_object')
    return JsonResponse.create(StatusCode.OK, {'url': url})


def get_detached_upload_file_url(job_id):
    """ Gets the signed url of the upload file for the given detached generation job.

        Args:
            job_id: the ID of the detached generation job to get the url for

        Returns:
            A signed URL to S3 of the specified file when not run locally. The path to the file when run locally.
            Error response if the job ID doesn't exist or isn't a detached job.
    """
    sess = GlobalDB.db().session
    file_job = sess.query(Job).filter(Job.job_id == job_id).first()
    if not file_job:
        return JsonResponse.error(ValueError('This job does not exist.'), StatusCode.CLIENT_ERROR)
    if file_job.submission_id:
        return JsonResponse.error(ValueError('This is not a detached generation job.'), StatusCode.CLIENT_ERROR)

    split_name = file_job.filename.split('/')
    if CONFIG_BROKER['local']:
        # when local, can just grab the filename because it stores the entire path
        url = os.path.join(CONFIG_BROKER['broker_files'], split_name[-1])
    else:
        url = S3Handler().get_signed_url(split_name[0], split_name[1],
                                         url_mapping=CONFIG_BROKER['submission_bucket_mapping'], method='get_object')
    return JsonResponse.create(StatusCode.OK, {'url': url})


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
