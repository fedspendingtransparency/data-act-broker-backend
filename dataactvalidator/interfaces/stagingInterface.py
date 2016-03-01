from sqlalchemy.exc import ResourceClosedError
from dataactcore.models.stagingInterface import StagingInterface as BaseStagingInterface

class StagingInterface(BaseStagingInterface):
    """ Manages all interaction with the staging database """

    #@staticmethod
    def dropTable(self,table):
        """

        Args:
            table: Table to be dropped

        Returns:
            True if successful
        """
        try:
            self.runStatement("DROP TABLE "+table)
        except Exception as e:
            # Table was not found
            pass

        try:
            self.session.close()
        except ResourceClosedError:
            # Connection already closed
            pass
        return True


    def tableExists(self,table):
        """ True if table exists, false otherwise """
        return self.engine.dialect.has_table(self.engine.connect(),table)

    def countRows(self,table):
        """ Returns number of rows in the specified table """
        if(self.tableExists(table)):
            response =  (self.runStatement("SELECT COUNT(*) FROM "+table)).fetchone()[0]
            # Try to prevent blocking
            self.session.close()
            return response
        else:
            return 0
