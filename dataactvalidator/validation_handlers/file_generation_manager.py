import logging

from datetime import datetime

from dataactbroker.helpers.generation_helper import (
    retrieve_cached_file_request, update_validation_job_info, d_file_query)

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status
from dataactcore.models.domainModels import ExecutiveCompensation
from dataactcore.models.jobModels import FileRequest, Submission
from dataactcore.models.stagingModels import AwardFinancialAssistance, AwardProcurement
from dataactcore.utils import fileD1, fileD2, fileE, fileF
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactvalidator.filestreaming.csv_selection import write_csv, write_query_to_file
from dataactvalidator.validation_handlers.validationError import ValidationError

logger = logging.getLogger(__name__)


class FileGenerationManager:
    """ Responsible for managing the generation of all files.

        Attributes:
            sess: Current database session
            job: he generation's upload Job
            agency_code: The CGAC or FREC code for the agency to generate the file for
            agency_type: The type of agency (awarding or funding) to generate the file for
            is_local: A boolean flag indicating whether the application is being run locally or not
    """
    def __init__(self, job, agency_code, agency_type, is_local=True):
        """ Initialize the FileGeneration Manager. Also derives the agency_code from Submission if this is a
            within-submission generation.

            Args:
                job: the generation's upload Job
                agency_code: The CGAC or FREC code for the agency to generate the file for
                agency_type: The type of agency (awarding or funding) to generate the file for
                is_local: A boolean flag indicating whether the application is being run locally or not
        """
        self.sess = GlobalDB.db().session
        self.job = job
        self.agency_code = agency_code
        self.agency_type = agency_type
        self.is_local = is_local

        if job.submission_id:
            submission = self.sess.query(Submission).filter_by(submission_id=job.submission_id).one_or_none()
            if submission:
                self.agency_code = submission.frec_code if submission.frec_code else submission.cgac_code

    def generate_from_job(self):
        """ Generates a file for a specified job """
        # Mark Job as running
        mark_job_status(self.job.job_id, 'running')

        # Ensure this is a file generation job
        job_type = self.job.job_type.name
        if job_type != 'file_upload':
            raise ResponseException(
                'Job ID {} is not a file generation job (job type is {})'.format(self.job.job_id, job_type),
                StatusCode.CLIENT_ERROR, None, ValidationError.jobError)

        # Ensure there is an available agency_code
        if not self.agency_code:
            raise ResponseException(
                'An agency_code must be provided to generate a file'.format(self.job.job_id, job_type),
                StatusCode.CLIENT_ERROR, None, ValidationError.jobError)

        # Retrieve any FileRequest that may have started since the Broker sent the request to SQS
        skip_generation = None
        if self.job.file_type.letter_name in ['D1', 'D2']:
            skip_generation = retrieve_cached_file_request(self.job, self.agency_type, self.agency_code, self.is_local)

        if not skip_generation:
            # Generate timestamped file names
            raw_filename = CONFIG_BROKER["".join([str(self.job.file_type.name), "_file_name"])]
            self.job.original_filename = S3Handler.get_timestamped_filename(raw_filename)
            if self.is_local:
                self.job.filename = "".join([CONFIG_BROKER['broker_files'], self.job.original_filename])
            else:
                self.job.filename = "".join([str(self.job.submission_id), "/", self.job.original_filename])
            self.sess.commit()

            # Generate the file, and upload to S3
            if self.job.file_type.letter_name in ['D1', 'D2']:
                # Update the validation Job if necessary
                if self.job.submission_id:
                    update_validation_job_info(self.sess, self.job)

                self.generate_d_file()
            elif self.job.file_type.letter_name == 'E':
                self.generate_e_file()
            else:
                self.generate_f_file()

            mark_job_status(self.job.job_id, 'finished')

        logger.info({
            'message': 'Finished file {} generation'.format(self.job.file_type.letter_name),
            'message_type': 'ValidatorInfo', 'job_id': self.job.job_id, 'agency_code': self.agency_code,
            'file_type': self.job.file_type.letter_name, 'start_date': self.job.start_date,
            'end_date': self.job.end_date, 'filename': self.job.original_filename
        })

    def generate_d_file(self):
        """ Write file D1 or D2 to an appropriate CSV. """
        log_data = {'message': 'Starting file {} generation'.format(self.job.file_type.letter_name),
                    'message_type': 'ValidatorInfo', 'job_id': self.job.job_id, 'agency_code': self.agency_code,
                    'file_type': self.job.file_type.letter_name, 'start_date': self.job.start_date,
                    'end_date': self.job.end_date, 'filename': self.job.original_filename}
        if self.job.submission_id:
            log_data['submission_id'] = self.job.submission_id
        logger.info(log_data)

        # Get or create a FileRequest for this generation
        current_date = datetime.now().date()
        file_request_params = {
            "job_id": self.job.job_id, "is_cached_file": True, "start_date": self.job.start_date,
            "end_date": self.job.end_date, "agency_code": self.agency_code, "file_type": self.job.file_type.letter_name,
            "agency_type": self.agency_type
        }

        file_request = self.sess.query(FileRequest).filter_by(**file_request_params).one_or_none()
        if not file_request:
            file_request_params["request_date"] = current_date
            file_request = FileRequest(**file_request_params)
            self.sess.add(file_request)
            self.sess.commit()

        # Mark this Job as not from-cache, and mark the FileRequest as the cached version (requested today)
        self.job.from_cached = False
        file_request.is_cached_file = True
        file_request.request_date = current_date
        self.sess.commit()

        # Prepare file data
        file_utils = fileD1 if self.job.file_type.letter_name == 'D1' else fileD2
        local_file = "".join([CONFIG_BROKER['d_file_storage_path'], self.job.original_filename])
        headers = [key for key in file_utils.mapping]
        query_utils = {"file_utils": file_utils, "agency_code": self.agency_code, "agency_type": self.agency_type,
                       "start": self.job.start_date, "end": self.job.end_date, "sess": self.sess}

        # Generate the file and put in S3
        write_query_to_file(local_file, self.job.filename, headers, self.job.file_type.letter_name, self.is_local,
                            d_file_query, query_utils)
        log_data['message'] = 'Finished writing to file: {}'.format(self.job.original_filename)
        logger.info(log_data)

    def generate_e_file(self):
        """ Write file E to an appropriate CSV. """
        log_data = {'message': 'Starting file E generation', 'message_type': 'ValidatorInfo', 'job_id': self.job.job_id,
                    'submission_id': self.job.submission_id, 'file_type': 'executive_compensation'}
        logger.info(log_data)

        d1 = self.sess.query(AwardProcurement.awardee_or_recipient_uniqu).\
            filter(AwardProcurement.submission_id == self.job.submission_id).\
            distinct()
        d2 = self.sess.query(AwardFinancialAssistance.awardee_or_recipient_uniqu).\
            filter(AwardFinancialAssistance.submission_id == self.job.submission_id).\
            distinct()
        duns_set = {r.awardee_or_recipient_uniqu for r in d1.union(d2)}
        duns_list = list(duns_set)    # get an order

        rows = []
        for i in range(0, len(duns_list), 100):
            rows.extend(fileE.retrieve_rows(duns_list[i:i + 100]))

        # Add rows to database here.
        # TODO: This is a temporary solution until loading from SAM's SFTP has been resolved
        for row in rows:
            self.sess.merge(ExecutiveCompensation(**fileE.row_to_dict(row)))
        self.sess.commit()

        log_data['message'] = 'Writing file E CSV'
        logger.info(log_data)

        write_csv(self.job.original_filename, self.job.filename, self.is_local, fileE.Row._fields, rows)

    def generate_f_file(self):
        """ Write rows from fileF.generate_f_rows to an appropriate CSV. """
        log_data = {'message': 'Starting file F generation', 'message_type': 'ValidatorInfo', 'job_id': self.job.job_id,
                    'submission_id': self.job.submission_id, 'file_type': 'sub_award'}
        logger.info(log_data)

        rows_of_dicts = fileF.generate_f_rows(self.job.submission_id)
        header = [key for key in fileF.mappings]    # keep order
        body = []
        for row in rows_of_dicts:
            body.append([row[key] for key in header])

        log_data['message'] = 'Writing file F CSV'
        logger.info(log_data)

        write_csv(self.job.original_filename, self.job.filename, self.is_local, header, body)
