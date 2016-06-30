from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.domainModels import CGAC
from dataactcore.models.validationModels import RuleSeverity, FileType
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

    def getRuleSeverityId(self, name):
        query = self.session.query(RuleSeverity).filter(RuleSeverity.name == name)
        result = self.runUniqueQuery(query, "No rule severity found for specified name", "Multiple rule severities found for specified name")
        return result.rule_severity_id

    def getFileTypeId(self, name):
        query = self.session.query(FileType).filter(FileType.name == name)
        result = self.runUniqueQuery(query, "No file type found for specified name", "Multiple file types found for specified name")
        return result.file_id

    def getCGACCode(self, agency_name):
        query = self.session.query(CGAC).filter(CGAC.agency_name == agency_name)
        result = self.runUniqueQuery(query, "No CGAC Code found for specified agency name", "Multiple CGAC codes found for specified agency name")
        return result.cgac_code
