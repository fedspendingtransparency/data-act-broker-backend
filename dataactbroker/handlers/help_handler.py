from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import ExternalDataLoadDate, ExternalDataType


def get_data_sources():
    """Gather the data sources and dates from the external_data_load_date table

    Returns:
        An object containing the names of the data sources as keys and the dates they were last loaded as values
    """
    sess = GlobalDB.db().session
    load_dates = (
        sess.query(ExternalDataLoadDate.last_load_date_end.label("load_date"), ExternalDataType.name.label("name"))
        .join(ExternalDataType, ExternalDataLoadDate.external_data_type_id == ExternalDataType.external_data_type_id)
        .all()
    )

    data_source_dates = {}
    for load_date in load_dates:
        data_source_dates[load_date.name] = load_date.load_date.strftime("%m/%d/%Y %H:%M:%S")

    return data_source_dates
