import os.path
from random import randint
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


def run_sql_rule(rule_file, staging_db, submission_id, *models):
    """Insert the models into the database (with a random submission id), then
    run the rule SQL against those models"""
    submission_id = submission_id or randint(1, 9999)
    sql = fetch_sql_tpl(rule_file).format(submission_id)

    for model in models:
        model.submission_id = submission_id
        staging_db.session.add(model)

    staging_db.session.commit()
    return staging_db.connection.execute(sql).rowcount
