import logging

from sqlalchemy import func

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import filename_fyp_sub_format
from dataactcore.broker_logging import configure_logging
from dataactcore.models.jobModels import (Comment, PublishedComment, FileType, CertifyHistory, PublishHistory,
                                          PublishedFilesHistory, Submission)

from dataactvalidator.filestreaming.csv_selection import write_stream_query
from dataactvalidator.health_check import create_app

from dataactcore.models.userModel import User  # noqa

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    sess = GlobalDB.db().session

    configure_logging()

    with create_app().app_context():
        # Unpublished comments files
        logger.info('Generating unpublished comments files')

        is_local = CONFIG_BROKER['local']

        headers = ['File', 'Comment']
        commented_submissions = sess.query(Comment.submission_id).distinct()

        for submission in commented_submissions:
            # Preparing for the comments files
            submission_id = submission.submission_id
            submission = sess.query(Submission).filter_by(submission_id=submission_id).one()
            filename = 'SubID-{}_comments_{}.csv'.format(submission_id, filename_fyp_sub_format(submission))
            local_file = "".join([CONFIG_BROKER['broker_files'], filename])
            file_path = local_file if is_local else '{}/{}'.format(str(submission_id), filename)

            unpublished_query = sess.query(FileType.name, Comment.comment).\
                join(FileType, Comment.file_type_id == FileType.file_type_id).\
                filter(Comment.submission_id == submission_id)

            # Generate the file locally, then place in S3
            write_stream_query(sess, unpublished_query, local_file, file_path, is_local, header=headers)

        logger.info('Finished generating unpublished comments files')

        # Published comments files
        logger.info('Copying published comments files')
        commented_pub_submissions = sess.query(PublishedComment.submission_id).distinct()

        for published_submission in commented_pub_submissions:
            submission_id = published_submission.submission_id
            submission = sess.query(Submission).filter_by(submission_id=submission_id).one()
            filename = 'SubID-{}_comments_{}.csv'.format(str(submission_id), filename_fyp_sub_format(submission))

            # See if we already have this published file in the list
            existing_pub_history = sess.query(PublishedFilesHistory).\
                filter(PublishedFilesHistory.submission_id == submission_id,
                       PublishedFilesHistory.filename.like('%' + filename)).one_or_none()

            # If we already have this file in the table, we don't need to make it again, we can skip
            if existing_pub_history:
                continue

            agency_code = submission.frec_code if submission.frec_code else submission.cgac_code
            # Get the latest publish+certify IDs
            max_pub_id = sess.query(func.max(PublishHistory.publish_history_id).label('pub_id')).\
                filter_by(submission_id=submission_id).one()
            max_cert_id = sess.query(func.max(CertifyHistory.certify_history_id).label('cert_id')). \
                filter_by(submission_id=submission_id).one()

            route_vars = [agency_code, submission.reporting_fiscal_year, submission.reporting_fiscal_period // 3,
                          max_pub_id.pub_id]
            new_route = '/'.join([str(var) for var in route_vars]) + '/'

            if not is_local:
                old_path = '{}/{}'.format(str(submission_id), filename)
                new_path = new_route + filename
                # Copy the file if it's a non-local submission
                S3Handler().copy_file(original_bucket=CONFIG_BROKER['aws_bucket'],
                                      new_bucket=CONFIG_BROKER['certified_bucket'], original_path=old_path,
                                      new_path=new_path)
            else:
                new_path = "".join([CONFIG_BROKER['broker_files'], filename])

            # add published history
            file_history = PublishedFilesHistory(publish_history_id=max_pub_id.pub_id,
                                                 certify_history_id=max_cert_id.cert_id,
                                                 submission_id=submission_id, filename=new_path,
                                                 file_type_id=None, comment=None, warning_filename=None)
            sess.add(file_history)
        sess.commit()

        logger.info('Finished copying published comments files')
