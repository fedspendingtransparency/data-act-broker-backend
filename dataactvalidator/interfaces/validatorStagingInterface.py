from dataactcore.models.baseInterface import BaseInterface
from dataactcore.config import CONFIG_DB

class ValidatorStagingInterface(BaseInterface):
    """ Manages all interaction with the staging database """

    dbName = CONFIG_DB['staging_db_name']
    dbConfig = CONFIG_DB
    Session = None
    engine = None
    session = None

    def __init__(self):
        super(ValidatorStagingInterface,self).__init__()

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
        self.runStatement("".join(["DROP TABLE ",table]))
        self.session.commit()

    def tableExists(self,table):
        """ True if table exists, false otherwise """
        return self.engine.dialect.has_table(self.engine.connect(),table)

    def countRows(self,table):
        """ Returns number of rows in the specified table """
        if(self.tableExists(table)):
            response =  (self.runStatement("".join(["SELECT COUNT(*) FROM ",table]))).fetchone()[0]
            # Try to prevent blocking
            self.session.close()
            return response
        else:
            return 0

    @staticmethod
    def getTableName(jobId):
        """ Get the staging table name based on the job ID """
        return "".join(["job",str(jobId)])