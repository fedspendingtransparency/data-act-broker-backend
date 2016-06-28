from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.domainModels import CGAC
from dataactcore.models.validationModels import FileType
from dataactcore.config import CONFIG_DB


class ValidationInterface(BaseInterface):
    """Manages all interaction with the validation database."""
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

    def getSession(self):
        """ Return session object"""
        return self.session

    def getAllAgencies(self):
        """ Return all agencies """
        return self.session.query(CGAC).all()

    def getAgencyName(self, cgac_code):
        agency = self.session.query(CGAC).filter(CGAC.cgac_code == cgac_code).first()
        return agency.agency_name if agency is not None else None

    def getFileTypeById(self, id):
        """ Return name of file type """
        return self.getNameFromDict(FileType,"TYPE_DICT","name",id,"file_id")

    def getFileTypeIdByName(self, fileType):
        """ Return file type ID for given name """
        return self.getNameFromDict(FileType, "TYPE_ID_DICT", "file_id", fileType, "name")

    def getFileTypeList(self):
        """ Return list of file types """
        fileTypes = self.session.query(FileType.name).all()
        # Convert result into list
        return [fileType.name for fileType in fileTypes]