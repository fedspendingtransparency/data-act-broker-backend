import os.path
from datetime import datetime
from dataactcore.models.jobModels import Submission
from dataactvalidator.filestreaming.sqlLoader import SQLLoader


# @todo: There's extra logic in SQLLoader around FieldCleaning which we may
# need to run through here, too
def fetch_sql_tpl(filename):
    filename = os.path.join(SQLLoader.sql_rules_path, filename + ".sql")
    with open(filename, "rU") as f:
        return f.read()


def insert_submission(db, submission):
    db.session.add(submission)
    db.session.commit()
    return submission.submission_id


def run_sql_rule(rule_file, staging_db, submission=None, models=None):
    """Insert the models into the database (with a random submission id), then
    run the rule SQL against those models"""
    if submission is None:
        submission = Submission(user_id=1, reporting_start_date=datetime(2015, 10, 1),
                                reporting_end_date= datetime(2015, 10, 31))
    if models is None:
        models = []

    submission_id = insert_submission(staging_db, submission)
    sql = fetch_sql_tpl(rule_file).format(submission_id)

    for model in models:
        model.submission_id = submission_id
        staging_db.session.add(model)

    staging_db.session.commit()
    return staging_db.connection.execute(sql).rowcount
