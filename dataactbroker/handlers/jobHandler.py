from datetime import date

from dataactcore.models.jobModels import Job, Submission, FileType
from dataactcore.models.jobTrackerInterface import JobTrackerInterface
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from sqlalchemy import and_

from dataactcore.interfaces.db import GlobalDB

from dataactcore.models.lookups import JOB_TYPE_DICT, FILE_TYPE_DICT

class JobHandler(JobTrackerInterface):
    """ Responsible for all interaction with the job tracker database
    Class fields:
    metaDataFieldMap -- Maps names in the request object to database column names

    Instance fields:
    engine -- sqlalchemy engine for generating connections and sessions
    connection -- sqlalchemy connection for executing direct SQL statements
    session -- sqlalchemy session for ORM usage
    """




