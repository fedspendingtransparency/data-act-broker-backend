import boto3
import logging
import smart_open

from contextlib import contextmanager
from datetime import datetime
from flask import Flask

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status
from dataactcore.models.domainModels import ExecutiveCompensation
from dataactcore.models.jobModels import Job, FileRequest, FPDSUpdate
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_STATUS_DICT_ID, JOB_TYPE_DICT
from dataactcore.models.stagingModels import AwardFinancialAssistance, AwardProcurement
from dataactcore.utils import fileD1, fileD2, fileE, fileF

from dataactvalidator.filestreaming.csv_selection import write_csv, write_query_to_file, stream_file_to_s3

logger = logging.getLogger(__name__)

STATUS_MAP = {"waiting": "waiting", "ready": "invalid", "running": "waiting", "finished": "finished",
              "invalid": "failed", "failed": "failed"}
VALIDATION_STATUS_MAP = {"waiting": "waiting", "ready": "waiting", "running": "waiting", "finished": "finished",
                         "failed": "failed", "invalid": "failed"}


@contextmanager
def job_context(job_id, agency_type, is_local=True):
    """ Common context for files D1, D2, E, and F generation. Handles marking the job finished and/or failed

        Args:
            job_id: the ID of the submission job
            agency_type: The type of agency (awarding or funding) to generate the file for
            is_local: a boolean indicating whether this is being run in a local environment or not

        Yields:
            The current DB session
    """
    # Flask context ensures we have access to global.g
    with Flask(__name__).app_context():
        sess, job = retrieve_job_context_data(job_id)
        try:
            yield sess, job
            if not job.from_cached:
                # only mark completed jobs as done
                logger.info({'message': 'Marking job {} as finished'.format(job.job_id), 'job_id': job.job_id,
                             'message_type': 'ValidatorInfo'})
                mark_job_status(job.job_id, "finished")
        except Exception as e:
            # logger.exception() automatically adds traceback info
            logger.exception({'message': 'Marking job {} as failed'.format(job.job_id), 'job_id': job.job_id,
                              'message_type': 'ValidatorException', 'exception': str(e)})

            # mark job as failed
            job.error_message = str(e)
            mark_job_status(job.job_id, "failed")

            # ensure FileRequest from failed job is not cached
            file_request = sess.query(FileRequest).filter_by(job_id=job.job_id, agency_type=agency_type,
                                                             start_date=job.start_date, end_date=job.end_date).\
                one_or_none()
            if file_request and file_request.is_cached_file:
                file_request.is_cached_file = False

            sess.commit()

        finally:
            file_request = sess.query(FileRequest).filter_by(job_id=job.job_id, agency_type=agency_type,
                                                             start_date=job.start_date, end_date=job.end_date).\
                one_or_none()
            if file_request and file_request.is_cached_file:
                # copy job data to all child FileRequests
                child_requests = sess.query(FileRequest).filter_by(parent_job_id=job.job_id, agency_type=agency_type,
                                                                   start_date=job.start_date, end_date=job.end_date).\
                    all()
                if len(child_requests) > 0:
                    logger.info({'message': 'Copying file data from job {} to its children'.format(job.job_id),
                                 'message_type': 'ValidatorInfo', 'job_id': job.job_id})
                    for child in child_requests:
                        copy_parent_file_request_data(sess, child.job, job, is_local)
            GlobalDB.close()


def retrieve_job_context_data(job_id):
    """ Retrieves a DB session and the job object for a the job_context function
        This needs to be separated into its own function so we can Mock it during tests

        Args:
            job_id: ID of the job to retrieve

        Returns:
            sess: Current database session
            job: Job model based on the job_id
    """
    sess = GlobalDB.db().session
    job = sess.query(Job).filter(Job.job_id == job_id).one_or_none()

    return sess, job


def generate_d_file(sess, job, agency_code, agency_type, is_local=True, old_filename=None):
    """ Write file D1 or D2 to an appropriate CSV.

        Args:
            sess: Current database session
            job: Upload Job
            agency_code: FREC or CGAC code for generation
            agency_type: The type of agency (awarding or funding) to generate the file for (only used for D file
                    generation)
            is_local: True if in local development, False otherwise
            old_filename: Previous version of filename, in cases where reverting to old file is necessary
    """
    log_data = {'message_type': 'ValidatorInfo', 'job_id': job.job_id, 'file_type': job.file_type.letter_name,
                'agency_code': agency_code, 'start_date': job.start_date, 'end_date': job.end_date}
    if job.submission_id:
        log_data['submission_id'] = job.submission_id

    # find current date and date of last FPDS pull
    current_date = datetime.now().date()
    last_update = sess.query(FPDSUpdate).one_or_none()
    fpds_date = last_update.update_date if last_update else current_date

    # check if FileRequest already exists with this job_id, if not, create one
    file_request = None
    file_request_list = sess.query(FileRequest).filter(FileRequest.job_id == job.job_id).all()

    for fr in file_request_list:
        if fr.agency_type == agency_type and fr.start_date == job.start_date and fr.end_date == job.end_date:
            file_request = fr
        else:
            fr.is_cached_file = False
    sess.commit()

    if not file_request:
        file_request = FileRequest(request_date=current_date, job_id=job.job_id, start_date=job.start_date,
                                   end_date=job.end_date, agency_code=agency_code, agency_type=agency_type,
                                   is_cached_file=False, file_type=job.file_type.letter_name)
        sess.add(file_request)

    # determine if anything needs to be done at all
    exists = file_request.is_cached_file
    if exists and not (job.file_type.letter_name == 'D1' and file_request.request_date < fpds_date):
        # this is the up-to-date cached version of the generated file
        # reset the file names on the upload Job
        log_data['message'] = '{} file has already been generated by this job'.format(job.file_type.letter_name)
        logger.info(log_data)

        filepath = CONFIG_BROKER['broker_files'] if is_local else "".join([str(job.submission_id), "/"])
        job.filename = "".join([filepath, old_filename])
        job.original_filename = old_filename
        job.from_cached = False

        if job.submission_id:
            # reset the file names on the validation job
            val_job = sess.query(Job).filter(Job.submission_id == job.submission_id,
                                             Job.file_type_id == job.file_type_id,
                                             Job.job_type_id == JOB_TYPE_DICT['csv_record_validation']).one_or_none()
            if val_job:
                val_job.filename = "".join([filepath, old_filename])
                val_job.original_filename = old_filename

        sess.commit()
    else:
        # search for potential parent FileRequests
        parent_file_request = None
        if not exists:
            # attempt to retrieve a parent request
            parent_file_requests = sess.query(FileRequest).\
                filter(FileRequest.file_type == job.file_type.letter_name, FileRequest.start_date == job.start_date,
                       FileRequest.end_date == job.end_date, FileRequest.agency_code == agency_code,
                       FileRequest.agency_type == agency_type, FileRequest.is_cached_file.is_(True)).all()

            # there will, very rarely, be more than one value in parent_file_requests
            for parent_request in parent_file_requests:
                valid_cached_job_statuses = [JOB_STATUS_DICT["running"], JOB_STATUS_DICT["finished"]]
                parent_job = sess.query(Job).filter_by(job_id=parent_request.job_id).one_or_none()

                # check that D1 FileRequests are newer than the last FPDS pull
                invalid_d1 = parent_request.file_type == 'D1' and parent_request.request_date < fpds_date

                # check FileRequest hasn't expired and Job status is valid
                invalid_job = not parent_job or parent_job.job_status_id not in valid_cached_job_statuses

                # check that this parent_request is newer than any previous valid requests
                is_older_request = parent_file_request and parent_request.updated_at <= parent_file_request.updated_at

                # if this parent_request is not a valid cached FileRequest
                if invalid_d1 or invalid_job or is_older_request:
                    # uncache FileRequest
                    parent_request.is_cached_file = False
                    continue

                # uncache outdated parent FileRequests
                if parent_file_request:
                    parent_file_request.is_cached_file = False

                # mark FileRequest with parent job_id
                parent_file_request = parent_request
                file_request.parent_job_id = parent_file_request.job_id

        sess.commit()

        if parent_file_request:
            # parent exists; copy parent data to this job
            copy_parent_file_request_data(sess, file_request.job, parent_file_request.job, is_local)
        else:
            # no cached file, or cached file is out-of-date
            log_data['message'] = 'Starting file {} generation'.format(job.file_type.letter_name)
            log_data['file_name'] = job.original_filename
            logger.info(log_data)

            # mark this Job as not from-cache, and mark the FileRequest as the cached version (requested today)
            job.from_cached = False
            file_request.is_cached_file = True
            file_request.request_date = current_date
            sess.commit()

            # actually generate the file
            file_utils = fileD1 if job.file_type.letter_name == 'D1' else fileD2
            local_file = "".join([CONFIG_BROKER['d_file_storage_path'], job.original_filename])
            headers = [key for key in file_utils.mapping]
            query_utils = {"file_utils": file_utils, "agency_code": agency_code, "agency_type": agency_type,
                           "start": job.start_date, "end": job.end_date, "sess": sess}
            write_query_to_file(local_file, job.filename, headers, job.file_type.letter_name, is_local, d_file_query,
                                query_utils)
            log_data['message'] = 'Finished writing to file: {}'.format(job.original_filename)
            logger.info(log_data)

    log_data['message'] = 'Finished file {} generation'.format(job.file_type.letter_name)
    logger.info(log_data)


def generate_f_file(sess, job, is_local):
    """Write rows from fileF.generate_f_rows to an appropriate CSV.

        Args:
            sess: database session
            job: upload Job
            is_local: True if in local development, False otherwise
    """
    log_data = {'message': 'Starting file F generation', 'message_type': 'ValidatorInfo', 'job_id': job.job_id,
                'submission_id': job.submission_id, 'file_type': 'sub_award'}
    logger.info(log_data)

    rows_of_dicts = fileF.generate_f_rows(job.submission_id)
    header = [key for key in fileF.mappings]    # keep order
    body = []
    for row in rows_of_dicts:
        body.append([row[key] for key in header])

    log_data['message'] = 'Writing file F CSV'
    logger.info(log_data)
    write_csv(job.original_filename, job.filename, is_local, header, body)

    log_data['message'] = 'Finished file F generation'
    logger.info(log_data)


def generate_e_file(sess, job, is_local):
    """Write file E to an appropriate CSV.

        Args:
            sess: database session
            job: upload Job
            is_local: True if in local development, False otherwise
    """
    log_data = {'message': 'Starting file E generation', 'message_type': 'ValidatorInfo', 'job_id': job.job_id,
                'submission_id': job.submission_id, 'file_type': 'executive_compensation'}
    logger.info(log_data)

    d1 = sess.query(AwardProcurement.awardee_or_recipient_uniqu).\
        filter(AwardProcurement.submission_id == job.submission_id).\
        distinct()
    d2 = sess.query(AwardFinancialAssistance.awardee_or_recipient_uniqu).\
        filter(AwardFinancialAssistance.submission_id == job.submission_id).\
        distinct()
    duns_set = {r.awardee_or_recipient_uniqu for r in d1.union(d2)}
    duns_list = list(duns_set)    # get an order

    rows = []
    for i in range(0, len(duns_list), 100):
        rows.extend(fileE.retrieve_rows(duns_list[i:i + 100]))

    # Add rows to database here.
    # TODO: This is a temporary solution until loading from SAM's SFTP has been resolved
    for row in rows:
        sess.merge(ExecutiveCompensation(**fileE.row_to_dict(row)))
    sess.commit()

    log_data['message'] = 'Writing file E CSV'
    logger.info(log_data)
    write_csv(job.original_filename, job.filename, is_local, fileE.Row._fields, rows)

    log_data['message'] = 'Finished file E generation'
    logger.info(log_data)


def d_file_query(query_utils, page_start, page_end):
    """ Retrieve D1 or D2 data.

        Args:
            query_utils: object containing:
                file_utils: fileD1 or fileD2 utils
                sess: database session
                agency_code: FREC or CGAC code for generation
                start: beginning of period for D file
                end: end of period for D file
            page_start: beginning of pagination
            page_end: end of pagination

        Return:
            paginated D1 or D2 query results
    """
    rows = query_utils["file_utils"].query_data(query_utils["sess"], query_utils["agency_code"],
                                                query_utils["agency_type"], query_utils["start"], query_utils["end"],
                                                page_start, page_end)
    return rows.all()


def copy_parent_file_request_data(sess, child_job, parent_job, is_local):
    """ Parent FileRequest job data to the child FileRequest job data.

        Args:
            sess: current DB session
            child_job: Job object for the child FileRequest
            parent_job: Job object for the parent FileRequest
            is_local: True if in local development, False otherwise
    """
    # Do not edit submissions that have successfully completed
    if child_job.job_status_id == JOB_STATUS_DICT['finished']:
        return

    # Keep path but update file name
    filename = '{}/{}'.format(child_job.filename.rsplit('/', 1)[0], parent_job.original_filename)

    # Copy parent job's data
    child_job.from_cached = True
    child_job.filename = filename
    child_job.original_filename = parent_job.original_filename
    child_job.number_of_errors = parent_job.number_of_errors
    child_job.number_of_warnings = parent_job.number_of_warnings
    child_job.error_message = parent_job.error_message

    # Change the validation job's file data when within a submission
    if child_job.submission_id is not None:
        val_job = sess.query(Job).filter(Job.submission_id == child_job.submission_id,
                                         Job.file_type_id == parent_job.file_type_id,
                                         Job.job_type_id == JOB_TYPE_DICT['csv_record_validation']).one()
        val_job.filename = filename
        val_job.original_filename = parent_job.original_filename
    sess.commit()

    copy_file_from_parent_to_child(child_job, parent_job, is_local)

    # Mark job status last so the validation job doesn't start until everything is done
    mark_job_status(child_job.job_id, JOB_STATUS_DICT_ID[parent_job.job_status_id])


def copy_file_from_parent_to_child(child_job, parent_job, is_local):
    """ Copy the file from the parent job's bucket to the child job's bucket.

        Args:
            child_job: Job object for the child FileRequest
            parent_job: Job object for the parent FileRequest
            is_local: True if in local development, False otherwise
    """
    file_type = parent_job.file_type.letter_name
    log_data = {'message': 'Copying data from parent job with job_id:{}'.format(parent_job.job_id),
                'message_type': 'ValidatorInfo', 'job_id': child_job.job_id, 'file_type': parent_job.file_type.name}

    if not is_local and parent_job.filename != child_job.filename:
        # Check to see if the same file exists in the child bucket
        s3 = boto3.client('s3', region_name=CONFIG_BROKER["aws_region"])
        response = s3.list_objects_v2(Bucket=CONFIG_BROKER['aws_bucket'], Prefix=child_job.filename)
        for obj in response.get('Contents', []):
            if obj['Key'] == child_job.filename:
                # The file already exists in this location
                log_data['message'] = 'Cached {} file CSV already exists in this location'.format(file_type)
                logger.info(log_data)
                return

        # Copy the parent file into the child's S3 location
        log_data['message'] = 'Copying the cached {} file from job {}'.format(file_type, parent_job.job_id)
        logger.info(log_data)
        with smart_open.smart_open(S3Handler.create_file_path(parent_job.filename), 'r') as reader:
            stream_file_to_s3(child_job.filename, reader)
