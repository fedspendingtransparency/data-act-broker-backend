import logging

from sqlalchemy import func

from dataactcore.interfaces.db import GlobalDB
from dataactcore.broker_logging import configure_logging
from dataactcore.models.jobModels import Submission, PublishHistory, Comment, PublishedComment

from dataactvalidator.health_check import create_app

from dataactcore.models.userModel import User  # noqa

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    sess = GlobalDB.db().session

    configure_logging()

    with create_app().app_context():
        logger.info('Moving published comments')

        # Create a CTE of the max publish history date for DABS submissions
        max_publish_history = sess.query(func.max(PublishHistory.updated_at).label('max_updated_at'),
                                         PublishHistory.submission_id.label('submission_id')). \
            join(Submission, PublishHistory.submission_id == Submission.submission_id). \
            filter(Submission.is_fabs.is_(False)). \
            group_by(PublishHistory.submission_id).cte('max_publish_history')

        # Get all comments that were written before the latest publication for all published/updated submissions
        publish_history_list = sess.query(Comment). \
            join(max_publish_history, max_publish_history.c.submission_id == Comment.submission_id). \
            filter(Comment.updated_at < max_publish_history.c.max_updated_at).\
            order_by(Comment.submission_id, Comment.file_type_id).all()

        # Create a list of comments and a list of all submissions involved
        comments_list = []
        submissions_list = []
        for obj in publish_history_list:
            tmp_obj = obj.__dict__
            tmp_obj.pop('_sa_instance_state')
            tmp_obj.pop('created_at')
            tmp_obj.pop('updated_at')
            tmp_obj.pop('comment_id')

            comments_list.append(tmp_obj)
            if tmp_obj['submission_id'] not in submissions_list:
                submissions_list.append(tmp_obj['submission_id'])

        # Delete all comments from the submissions we're inserting for
        sess.query(PublishedComment).filter(PublishedComment.submission_id.in_(submissions_list)).\
            delete(synchronize_session=False)

        # Save all the objects in the published comment table
        sess.bulk_save_objects([PublishedComment(**comment) for comment in comments_list])
        sess.commit()

        logger.info('Published comments moved')
