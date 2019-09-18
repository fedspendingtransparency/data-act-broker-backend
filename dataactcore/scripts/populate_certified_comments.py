import logging

from sqlalchemy import func

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.jobModels import Submission, CertifyHistory, Comment, CertifiedComment

from dataactvalidator.health_check import create_app

from dataactcore.models.userModel import User  # noqa

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    sess = GlobalDB.db().session

    configure_logging()

    with create_app().app_context():
        logger.info('Moving certified comments')

        # Create a CTE of the max certify history date for DABS submissions
        max_certify_history = sess.query(func.max(CertifyHistory.updated_at).label('max_updated_at'),
                                         CertifyHistory.submission_id.label('submission_id')). \
            join(Submission, CertifyHistory.submission_id == Submission.submission_id). \
            filter(Submission.d2_submission.is_(False)). \
            group_by(CertifyHistory.submission_id).cte('max_certify_history')

        # Get all comments that were written before the latest certification for all certified/updated submissions
        certify_history_list = sess.query(Comment). \
            join(max_certify_history, max_certify_history.c.submission_id == Comment.submission_id). \
            filter(Comment.updated_at < max_certify_history.c.max_updated_at).\
            order_by(Comment.submission_id, Comment.file_type_id).all()

        # Create a list of comments and a list of all submissions involved
        comments_list = []
        submissions_list = []
        for obj in certify_history_list:
            tmp_obj = obj.__dict__
            tmp_obj.pop('_sa_instance_state')
            tmp_obj.pop('created_at')
            tmp_obj.pop('updated_at')
            tmp_obj.pop('comment_id')

            comments_list.append(tmp_obj)
            if tmp_obj['submission_id'] not in submissions_list:
                submissions_list.append(tmp_obj['submission_id'])

        # Delete all comments from the submissions we're inserting for
        sess.query(CertifiedComment).filter(CertifiedComment.submission_id.in_(submissions_list)).\
            delete(synchronize_session=False)

        # Save all the objects in the certified comment table
        sess.bulk_save_objects([CertifiedComment(**comment) for comment in comments_list])
        sess.commit()

        logger.info('Certified comments moved')
