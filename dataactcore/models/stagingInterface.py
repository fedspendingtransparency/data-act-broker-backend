from dataactcore.models.baseInterface import BaseInterface
from dataactcore.utils.jobQueue import JobQueue
from dataactcore.config import CONFIG_DB


class StagingInterface(BaseInterface):
    """Manages all interaction with the job tracker database."""
    dbConfig = CONFIG_DB
    dbName = dbConfig['staging_db_name']
    Session = None
    engine = None
    session = None

    def __init__(self):
        self.dbName = self.dbConfig['staging_db_name']
        self.jobQueue = JobQueue()
        super(StagingInterface, self).__init__()

    @staticmethod
    def getDbName():
        """ Return database name"""
        return StagingInterface.dbName