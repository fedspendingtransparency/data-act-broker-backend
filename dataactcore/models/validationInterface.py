from sqlalchemy.orm import subqueryload

from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.domainModels import CGAC
from dataactcore.models.validationModels import (
    FileColumn, FileTypeValidation, RuleSeverity)
from dataactcore.models.stagingModels import AwardFinancialAssistance, AwardFinancial, Appropriation, ObjectClassProgramActivity, AwardProcurement
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner


class ValidationInterface(BaseInterface):
    """Manages all interaction with the validation/staging database."""
    MODEL_MAP = {"award": AwardFinancialAssistance, "award_financial": AwardFinancial, "appropriations": Appropriation,
                 "program_activity": ObjectClassProgramActivity, "award_procurement": AwardProcurement}

    def __init__(self):
        super(ValidationInterface, self).__init__()

    def getAgencyName(self, cgac_code):
        agency = self.session.query(CGAC).filter(CGAC.cgac_code == cgac_code).first()
        return agency.agency_name if agency is not None else None

    def getFileTypeById(self, id):
        """ Return name of file type """
        return self.getNameFromDict(FileTypeValidation,"TYPE_DICT","name",id,"file_id")

    def getFileTypeIdByName(self, fileType):
        """ Return file type ID for given name """
        return self.getNameFromDict(FileTypeValidation, "TYPE_ID_DICT", "file_id", fileType, "name")

    def getFileTypeList(self):
        """ Return list of file types """
        fileTypes = self.session.query(FileTypeValidation.name).all()
        # Convert result into list
        return [fileType.name for fileType in fileTypes]

    def getRuleSeverityId(self, name):
        """ Return rule severity ID for this name """
        return self.getNameFromDict(RuleSeverity, "SEVERITY_DICT", "rule_severity_id", name, "name")

    def insertSubmissionRecordByFileType(self, record, fileType):
        """Insert a submitted file row into its corresponding staging table

        Args:
            record: record to insert (Dict)
            submissionId: submissionId associated with this record (int)
            fileType: originating file type of the record (string)
        """

        rec = self.getModel(fileType)(**record)
        self.session.add(rec)
        self.session.commit()

    def getModel(self, fileType):
        """ Get model object for specified file type """
        if fileType not in self.MODEL_MAP:
            raise ResponseException("Not found in model map: {}".format(fileType), StatusCode.INTERNAL_ERROR, KeyError)
        return self.MODEL_MAP[fileType]

    def clearFileBySubmission(self, submissionId, fileType):
        """ Remove existing records for a submission ID and file type, done for updated submissions

        Args:
            submissionId: (int) ID of submission to be cleared
            fileType: (str) File type to clear
        """
        # Get model name based on file type
        model = self.getModel(fileType)
        # Delete existing records for this model
        self.session.query(model).filter(model.submission_id == submissionId).delete()
        self.session.commit()

    def getFieldsByFileList(self, fileType):
        """ Returns a list of valid field names that can appear in this type of file

        Args:
        fileType -- One of the set of valid types of files (e.g. Award, AwardFinancial)

        Returns:
        list of names
        """
        fileId = self.getFileTypeIdByName(fileType)
        if (fileId is None):
            raise ValueError("Filetype does not exist")
        queryResult = self.session.query(FileColumn).filter(FileColumn.file_id == fileId).all()
        for result in queryResult:
            result.name = FieldCleaner.cleanString(result.name)  # Standardize field names
            result.name_short = FieldCleaner.cleanString(result.name_short)
        return queryResult

    def getFieldsByFile(self, fileType, shortCols=False):
        """ Returns a dict of valid field names that can appear in this type of file

        Args:
        fileType -- One of the set of valid types of files (e.g. Award, AwardFinancial)
        shortCols -- If true, return the short column names instead of the long names

        Returns:
        dict with field names as keys and values are ORM object FileColumn
        """
        returnDict = {}
        fileId = self.getFileTypeIdByName(fileType)
        if (fileId is None):
            raise ValueError("File type does not exist")
        queryResult = self.session.query(FileColumn).options(subqueryload("field_type")).filter(
            FileColumn.file_id == fileId).all()
        for column in queryResult:
            if shortCols:
                returnDict[FieldCleaner.cleanString(column.name_short)] = column
            else:
                returnDict[FieldCleaner.cleanString(column.name)] = column
        return returnDict

    def getFileColumnsByFile(self, fileType):
        """ Returns a list of File Column objects that appear in this type of file

        Args:
        fileType -- One of the set of valid types of files (e.g. Award, AwardFinancial)

        Returns:
        dict with field names as keys and values are ORM object FileColumn
        """
        fileId = self.getFileTypeIdByName(fileType)
        if (fileId is None):
            raise ValueError("File type does not exist")
        return self.session.query(FileColumn).options(subqueryload("field_type")).filter(
            FileColumn.file_id == fileId).all()

    def getLongToShortColname(self):
        """Return a dictionary that maps schema field names to shorter, machine-friendly versions."""
        query = self.session.query(FileColumn.name, FileColumn.name_short).all()
        dict = {row.name: row.name_short for row in query}
        return dict

    def getShortToLongColname(self):
        """Return a dictionary that maps short, machine-friendly schema names to their long versions."""
        query = self.session.query(FileColumn.name, FileColumn.name_short).all()
        dict = {row.name_short: row.name for row in query}
        return dict
