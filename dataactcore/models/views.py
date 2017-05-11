from sqlalchemy import MetaData, Table
from dataactcore.interfaces.db import GlobalDB


class SubmissionUpdatedView:
    def __init__(self):
        """ Create the SubmissionUpdatedView """
        db = GlobalDB.db()
        metadata = MetaData(bind=db.engine)

        new_view = Table('submission_updated_at_view', metadata, autoload=True, info=dict(is_view=True))

        self.table = new_view

        self.submission_id = new_view.columns.submission_id
        self.updated_at = new_view.columns.updated_at
