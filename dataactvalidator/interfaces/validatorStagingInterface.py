from sqlalchemy.exc import ResourceClosedError
from dataactcore.models.stagingInterface import StagingInterface as BaseStagingInterface

class ValidatorStagingInterface(BaseStagingInterface):
    """ Manages all interaction with the staging database """

    #@staticmethod
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
