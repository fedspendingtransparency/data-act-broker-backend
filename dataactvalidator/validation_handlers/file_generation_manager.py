import logging

from dataactbroker.helpers.generation_helper import d_file_query, copy_file_generation_to_job

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import ExecutiveCompensation
from dataactcore.models.jobModels import Job
from dataactcore.models.lookups import FILE_TYPE_DICT_LETTER_NAME
from dataactcore.models.stagingModels import AwardFinancialAssistance, AwardProcurement
from dataactcore.utils import fileD1, fileD2, fileE, fileF

from dataactvalidator.filestreaming.csv_selection import write_csv, write_query_to_file

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

    def generate_file(self):
        """ Generates a file based on the FileGeneration object and updates any Jobs referencing it """
        raw_filename = CONFIG_BROKER["".join([FILE_TYPE_DICT_LETTER_NAME[self.file_type], "_file_name"])]
        file_name = S3Handler.get_timestamped_filename(raw_filename)
        if self.is_local:
            file_path = "".join([CONFIG_BROKER['broker_files'], file_name])
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
        elif self.file_type == 'E':
            self.generate_e_file()
            log_data['job_id'] = self.job.job_id
        elif self.file_type == 'F':
            self.generate_f_file()
            log_data['job_id'] = self.job.job_id
        else:
            e = 'No FileGeneration object for D file generation.' if self.file_type in ['D1', 'D2'] else \
                'Cannot generate file for {} file type.'.format(self.file_type if self.file_type else 'empty')
            raise Exception(e)

        logger.info(log_data)

    def generate_d_file(self, file_path):
        """ Write file D1 or D2 to an appropriate CSV. """
        log_data = {
            'message': 'Starting file {} generation'.format(self.file_type), 'message_type': 'ValidatorInfo',
            'agency_code': self.file_generation.agency_code, 'agency_type': self.file_generation.agency_type,
            'start_date': self.file_generation.start_date, 'end_date': self.file_generation.end_date,
            'file_generation_id': self.file_generation.file_generation_id, 'file_type': self.file_type,
            'file_path': file_path
        }
        logger.info(log_data)

        original_filename = file_path.split('/')[-1]
        local_file = "".join([CONFIG_BROKER['d_file_storage_path'], original_filename])

        # Prepare file data
        if self.file_type == 'D1':
            file_utils = fileD1
        elif self.file_type == 'D2':
            file_utils = fileD2
        else:
            raise Exception('Failed to generate_d_file with file_type:{} (must be D1 or D2).'.format(self.file_type))
        headers = [key for key in file_utils.mapping]
        query_utils = {
            "sess": self.sess, "file_utils": file_utils, "agency_code": self.file_generation.agency_code,
            "agency_type": self.file_generation.agency_type, "start": self.file_generation.start_date,
            "end": self.file_generation.end_date}

        # Generate the file locally, then place in S3
        write_query_to_file(local_file, file_path, headers, self.file_type, self.is_local, d_file_query,
                            query_utils)

        log_data['message'] = 'Finished writing to file: {}'.format(original_filename)
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
