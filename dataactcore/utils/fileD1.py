from collections import OrderedDict
from sqlalchemy import cast, Date

from dataactcore.models.stagingModels import DetachedAwardProcurement

file_model = DetachedAwardProcurement

mapping = OrderedDict([

])


def query_data(session, agency_code, start, end):
    rows = session.query(file_model).\
        filter(file_model.awarding_agency_code == agency_code).\
        filter(cast(file_model.action_date, Date) > start).\
        filter(cast(file_model.action_date, Date) < end)
    session.commit()
    return rows
