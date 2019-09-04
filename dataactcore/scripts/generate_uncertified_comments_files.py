import logging

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.jobModels import SubmissionNarrative, FileType

from dataactvalidator.filestreaming.csv_selection import write_stream_query
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    sess = GlobalDB.db().session

    configure_logging()

    with create_app().app_context():
        logger.info('Generating uncertified comments files')

        is_local = CONFIG_BROKER['local']

        headers = ['File', 'Comment']
        commented_submissions = sess.query(SubmissionNarrative.submission_id).distinct()

        for submission in commented_submissions:
            # Preparing for the comments files
            filename = 'submission_{}_comments.csv'.format(submission.submission_id)
            local_file = "".join([CONFIG_BROKER['broker_files'], filename])
            file_path = local_file if is_local else '{}/{}'.format(str(submission.submission_id), filename)

            uncertified_query = sess.query(FileType.name, SubmissionNarrative.narrative).\
                join(FileType, SubmissionNarrative.file_type_id == FileType.file_type_id).\
                filter(SubmissionNarrative.submission_id == submission.submission_id)

            # Generate the file locally, then place in S3
            write_stream_query(sess, uncertified_query, local_file, file_path, is_local, header=headers)

        logger.info('Finished generating uncertified comments files')
