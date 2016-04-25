from dataactcore.models.baseInterface import BaseInterface
from dataactcore.config import CONFIG_DB
from sqlalchemy import MetaData, Table
from sqlalchemy.exc import NoSuchTableError


class ValidatorStagingInterface(BaseInterface):
    """ Manages all interaction with the staging database """

    dbConfig = CONFIG_DB
    dbName = dbConfig['staging_db_name']
    Session = None
    engine = None
    session = None

    def __init__(self):
        self.dbName = self.dbConfig['staging_db_name']
        super(ValidatorStagingInterface, self).__init__()

    @staticmethod
    def getDbName():
        """ Return database name"""
        return ValidatorStagingInterface.dbName

    def dropTable(self,table):
        """

        Args:
            table: Table to be dropped

        Returns:
            True if successful
        """

        metadata = MetaData()
        stagingTable = Table(table, metadata, autoload_with=self.engine)
        stagingTable.drop(bind=self.engine)

    def tableExists(self,table):
        """ True if table exists, false otherwise """
        return self.engine.dialect.has_table(self.engine.connect(),table)

    def countRows(self,table):
        """ Returns number of rows in the specified table """
        metadata = MetaData()
        try:
            stagingTable = Table(table, metadata, autoload_with=self.engine)
        except NoSuchTableError:
            return 0
        rows = self.session.query(stagingTable).count()
        self.session.close()
        return rows

    @staticmethod
    def getTableName(jobId):
        """ Get the staging table name based on the job ID """
        return "".join(["job",str(jobId)])