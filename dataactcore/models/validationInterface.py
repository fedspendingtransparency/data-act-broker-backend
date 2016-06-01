from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.domainModels import CGAC
from dataactcore.config import CONFIG_DB


class ValidationInterface(BaseInterface):
    """Manages all interaction with the user database."""
    dbConfig = CONFIG_DB
    dbName = dbConfig['validator_db_name']
    Session = None
    engine = None
    session = None

    def __init__(self):
        self.dbName = self.dbConfig['validator_db_name']
        super(ValidationInterface, self).__init__()

    @staticmethod
    def getDbName():
        """ Return database name"""
        return ValidationInterface.dbName

    def getAllAgencies(self):
        """ Return all agencies """
        return self.session.query(CGAC).all()
