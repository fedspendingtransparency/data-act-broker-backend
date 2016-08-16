from random import randint

from dataactvalidator.filestreaming.sqlLoader import SQLLoader


def error_rows(rule_file, staging_db, *models):
    """Insert the models into the database (with a random submission id), then
    run the rule SQL against those models"""
    submission_id = randint(1, 9999)
    sql = SQLLoader.readSqlStr(rule_file).format(submission_id)

    for model in models:
        model.submission_id = submission_id
        staging_db.session.add(model)

    staging_db.session.commit()

    return staging_db.connection.execute(sql).fetchall()


def number_of_errors(rule_file, staging_db, *models):
    return len(error_rows(rule_file, staging_db, *models))


def query_columns(rule_file, staging_db):
    sql = SQLLoader.readSqlStr(rule_file).format(randint(1, 9999))
    return staging_db.connection.execute(sql).keys()
