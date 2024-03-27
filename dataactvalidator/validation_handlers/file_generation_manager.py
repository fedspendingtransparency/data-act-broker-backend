import logging

from dateutil.relativedelta import relativedelta

from dataactbroker.helpers.generation_helper import a_file_query, d_file_query, copy_file_generation_to_job

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.function_bag import (mark_job_status, filename_fyp_sub_format, filename_fyp_format,
                                                 get_timestamp)
from dataactcore.models.jobModels import Job
from dataactcore.models.lookups import DETACHED_FILENAMES, SUBMISSION_FILENAMES
from dataactcore.utils import fileA, fileD1, fileD2, fileE_F
from dataactcore.utils.ResponseError import ResponseError

from dataactvalidator.filestreaming.csv_selection import write_stream_query

logger = logging.getLogger(__name__)


class FileGenerationManager:
    """ Responsible for managing the generation of all files.

        Attributes:
            sess: Current database session
            is_local: A boolean flag indicating whether the application is being run locally or not
            file_generation: FileGeneration object representing a D file generation task
            job: Job object for an E or F file generation task
            file_type: File type letter name
    """
    def __init__(self, sess, is_local, file_generation=None, job=None):
        """ Initialize the FileGeneration Manager.

            Args:
                sess: Current database session
                is_local: A boolean flag indicating whether the application is being run locally or not
                file_generation: FileGeneration object representing a D file generation task
                job: Job object for an E or F file generation task
        """
        self.sess = sess
        self.is_local = is_local
        self.file_generation = file_generation
        self.job = job
        self.file_type = job.file_type.letter_name if job else file_generation.file_type
        self.element_numbers = file_generation.element_numbers if file_generation else False

    def generate_file(self, agency_code=None):
        """ Generates a file based on the FileGeneration object and updates any Jobs referencing it """
        fillin_vals = {'timestamp': get_timestamp()}
        if self.file_generation:
            fillin_vals.update({
                'start': self.file_generation.start_date.strftime('%Y%m%d'),
                'end': self.file_generation.end_date.strftime('%Y%m%d'),
                'agency_type': self.file_generation.agency_type,
                'ext': '.{}'.format(self.file_generation.file_format),
            })
        if self.job and self.job.submission:
            # Submission Files
            if self.job.file_type.letter_name == 'A':
                file_name = self.job.original_filename
            else:
                fillin_vals.update({
                    'submission_id': self.job.submission_id,
                    'FYP': filename_fyp_sub_format(self.job.submission)
                })
                file_name = SUBMISSION_FILENAMES[self.file_type].format(**fillin_vals)
        else:
            # Detached Files
            if self.job and self.job.file_type.letter_name == 'A':
                period_date = self.job.end_date + relativedelta(months=3)
                fillin_vals['FYP'] = filename_fyp_format(period_date.year, period_date.month, False)
            file_name = DETACHED_FILENAMES[self.file_type].format(**fillin_vals)
        if self.is_local:
            file_path = "".join([CONFIG_BROKER['broker_files'], file_name])
        elif self.job and self.job.file_type.letter_name == 'A' and self.job.submission_id is not None:
            file_path = "".join(['{}/'.format(self.job.submission_id), file_name])
        else:
            file_path = "".join(["None/", file_name])

        # Generate the file and upload to S3
        log_data = {'message': 'Finished file {} generation'.format(self.file_type), 'message_type': 'ValidatorInfo',
                    'file_type': self.file_type, 'file_path': file_path}
        if self.file_generation:
            self.generate_d_file(file_path)

            log_data.update({
                'agency_code': self.file_generation.agency_code, 'agency_type': self.file_generation.agency_type,
                'start_date': self.file_generation.start_date, 'end_date': self.file_generation.end_date,
                'file_generation_id': self.file_generation.file_generation_id
            })
        elif self.job and self.job.file_type.letter_name in ['A', 'E', 'F']:
            log_data['job_id'] = self.job.job_id
            mark_job_status(self.job.job_id, 'running')

            if self.job.file_type.letter_name == 'A':
                if not agency_code:
                    raise ResponseError('Agency code not provided for an A file generation')

                self.generate_a_file(agency_code, file_path)
            else:
                # Call self.generate_%s_file() where %s is e or f based on the Job's file_type
                file_type_lower = self.job.file_type.letter_name.lower()
                getattr(self, 'generate_%s_file' % file_type_lower)()

            mark_job_status(self.job.job_id, 'finished')
        else:
            e = 'No FileGeneration object for D file generation.' if self.file_type in ['D1', 'D2'] else \
                'Cannot generate file for {} file type.'.format(self.file_type if self.file_type else 'empty')
            raise ResponseError(e)

        logger.info(log_data)

    def generate_d_file(self, file_path):
        """ Write file D1 or D2 to an appropriate CSV. """
        log_data = {
            'message': 'Starting file {} generation'.format(self.file_type), 'message_type': 'ValidatorInfo',
            'agency_code': self.file_generation.agency_code, 'agency_type': self.file_generation.agency_type,
            'start_date': self.file_generation.start_date, 'end_date': self.file_generation.end_date,
            'file_generation_id': self.file_generation.file_generation_id, 'file_type': self.file_type,
            'file_format': self.file_generation.file_format, 'file_path': file_path,
            'element_numbers': self.element_numbers
        }
        logger.info(log_data)

        original_filename = file_path.split('/')[-1]
        local_file = "".join([CONFIG_BROKER['d_file_storage_path'], original_filename])

        header_index = 0
        # Prepare file data
        if self.file_type == 'D1':
            file_utils = fileD1
            if self.file_generation.element_numbers:
                header_index = 1
        elif self.file_type == 'D2':
            file_utils = fileD2
        else:
            raise ResponseError('Failed to generate_d_file with file_type:{} (must be D1 or D2).'.format(
                self.file_type))
        headers = [val[header_index] for key, val in file_utils.mapping.items()]

        log_data['message'] = 'Writing {} file {}: {}'.format(self.file_type, self.file_generation.file_format.upper(),
                                                              original_filename)
        logger.info(log_data)

        query_utils = {
            "sess": self.sess, "file_utils": file_utils, "agency_code": self.file_generation.agency_code,
            "agency_type": self.file_generation.agency_type, "start": self.file_generation.start_date,
            "end": self.file_generation.end_date}
        logger.debug({'query_utils': query_utils})

        # Generate the file locally, then place in S3
        write_stream_query(self.sess, d_file_query(query_utils), local_file, file_path, self.is_local, header=headers,
                           file_format=self.file_generation.file_format)

        log_data['message'] = 'Finished writing {} file {}: {}'.format(self.file_type,
                                                                       self.file_generation.file_format.upper(),
                                                                       original_filename)
        logger.info(log_data)

        self.file_generation.file_path = file_path
        self.sess.commit()

        for job in self.sess.query(Job).filter_by(file_generation_id=self.file_generation.file_generation_id).all():
            copy_file_generation_to_job(job, self.file_generation, self.is_local)

    def generate_e_file(self):
        """ Write file E to an appropriate CSV. """
        log_data = {'message': 'Starting file E generation', 'message_type': 'ValidatorInfo', 'job_id': self.job.job_id,
                    'submission_id': self.job.submission_id, 'file_type': 'executive_compensation'}
        logger.info(log_data)

        file_e_sql = fileE_F.generate_file_e_sql(self.job.submission_id)

        log_data['message'] = 'Writing E file CSV: {}'.format(self.job.original_filename)
        logger.info(log_data)
        # Generate the file and put in S3
        write_stream_query(self.sess, file_e_sql, self.job.original_filename, self.job.filename, self.is_local,
                           generate_headers=True, generate_string=False)

        log_data['message'] = 'Finished writing E file CSV: {}'.format(self.job.original_filename)
        logger.info(log_data)

    def generate_f_file(self):
        """ Write rows from fileF.generate_f_rows to an appropriate CSV. """
        log_data = {'message': 'Starting file F generation', 'message_type': 'ValidatorInfo', 'job_id': self.job.job_id,
                    'submission_id': self.job.submission_id, 'file_type': 'sub_award'}
        logger.info(log_data)

        file_f_sql = fileE_F.generate_file_f_sql(self.job.submission_id)

        # writing locally first without uploading
        log_data['message'] = 'Writing F file CSV: {}'.format(self.job.original_filename)
        logger.info(log_data)
        # Generate the file and put in S3
        write_stream_query(self.sess, file_f_sql, self.job.original_filename, self.job.filename, self.is_local,
                           generate_headers=True, generate_string=False)

        log_data['message'] = 'Finished writing F file CSV: {}'.format(self.job.original_filename)
        logger.info(log_data)

    def generate_a_file(self, agency_code, file_path):
        """ Write file A to an appropriate CSV. """
        self.job.filename = file_path
        self.job.original_filename = file_path.split('/')[-1]
        self.sess.commit()

        log_data = {'message': 'Starting file A generation', 'message_type': 'ValidatorInfo', 'job_id': self.job.job_id,
                    'agency_code': agency_code, 'file_type': self.job.file_type.letter_name,
                    'start_date': self.job.start_date, 'end_date': self.job.end_date,
                    'filename': self.job.original_filename}
        logger.info(log_data)

        local_file = "".join([CONFIG_BROKER['d_file_storage_path'], self.job.original_filename])
        headers = [val[0] for key, val in fileA.mapping.items()]
        # add 3 months to account for fiscal year
        period_date = self.job.end_date + relativedelta(months=3)

        log_data['message'] = 'Writing A file CSV: {}'.format(self.job.original_filename)
        logger.info(log_data)

        query_utils = {"agency_code": agency_code, "period": period_date.month, "year": period_date.year,
                       "sess": self.sess}
        logger.debug({'query_utils': query_utils})

        # Generate the file and put in S3
        write_stream_query(self.sess, a_file_query(query_utils), local_file, self.job.filename, self.is_local,
                           header=headers)
        log_data['message'] = 'Finished writing A file CSV: {}'.format(self.job.original_filename)
        logger.info(log_data)
