from datetime import datetime
from random import randint

from dataactcore.models.jobModels import Submission
from dataactvalidator.filestreaming.sqlLoader import SQLLoader
from dataactcore.models.jobModels import PublishStatus
from dataactcore.models.lookups import PUBLISH_STATUS


def insert_submission(db, submission):
    db.session.add(submission)
    db.session.commit()
    return submission.submission_id


def error_rows(rule_file, staging_db, submission=None, models=None, assert_num=None):
    """Insert the models into the database, then run the rule SQL against
    those models. Return the resulting (invalid) rows"""
    if submission is None:
        submission = Submission(
            user_id=None, reporting_start_date=datetime(2015, 10, 1),
            reporting_end_date=datetime(2015, 10, 31))
    if models is None:
        models = []

    submission_id = insert_submission(staging_db, submission)
    sql = SQLLoader.read_sql_str(rule_file).format(submission_id)

    for model in models:
        model.submission_id = submission_id
        staging_db.session.add(model)

    staging_db.session.commit()
    result = staging_db.connection.execute(sql).fetchall()

    if assert_num is not None:
        assert(len(result) == assert_num)

    return result


def number_of_errors(rule_file, staging_db, submission=None, models=None, assert_num=None):
    return len(error_rows(rule_file, staging_db, submission, models, assert_num))


def query_columns(rule_file, staging_db):
    sql = SQLLoader.read_sql_str(rule_file).format(randint(1, 9999))
    return staging_db.connection.execute(sql).keys()


def populate_publish_status(database):
    for ps in PUBLISH_STATUS:
        status = PublishStatus(publish_status_id=ps.id, name=ps.name, description=ps.desc)
        database.session.merge(status)
    database.session.commit()
