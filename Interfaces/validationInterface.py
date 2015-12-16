import sqlalchemy
import json
from dataactcore.models import validationInterface
from sqlalchemy.orm import subqueryload
from dataactcore.models.validationModels import Rule, RuleType, FileColumn, FileType
from sqlalchemy.orm.exc import NoResultFound,MultipleResultsFound
from dataactcore.utils.responseException import ResponseException


class ValidationInterface(validationInterface.ValidationInterface) :
    """ Manages all interaction with the validation database
    """

    def getValidations(self,type):
        """ Get array of dicts for all validations of specified type
        Args:
        type -- type of validation to check for (e.g. single_record, cross_record, external)

        Returns:
        array of dicts, each representing a single validation
        """
        pass

    def getFieldsByFileList(self,filetype):
        """ Returns a list of valid field names that can appear in this type of file

        Args:
        filetype -- One of the set of valid types of files (e.g. Award, AwardFinancial)

        Returns:
        list of names
        """
        fileId = self.getFileId(filetype)
        returnList  = []
        if(fileId is None) :
            raise ValueError("Filetype does not exist")
        queryResult = self.session.query(FileColumn.name).filter(FileColumn.file_id == fileId).all()
        return queryResult

    def getFieldsByFile(self,filetype):
        """ Returns a dict of valid field names that can appear in this type of file

        Args:
        filetype -- One of the set of valid types of files (e.g. Award, AwardFinancial)

        Returns:
        dict with field names as keys and values are ORM object FileColumn
        """
        returnDict = {}
        fileId = self.getFileId(filetype)
        if(fileId is None) :
            raise ValueError("Filetype does not exist")
        queryResult = self.session.query(FileColumn).options(subqueryload("field_type")).filter(FileColumn.file_id == fileId).all()
        for column in queryResult :
            returnDict[column.name]  = column
        return returnDict


    def getFileId(self,filename) :
        queryResult = self.session.query(FileType).filter(FileType.name== filename).all()
        if(len(queryResult) > 0) :
            return queryResult[0].file_id
        return None

    def getRulesByFile(self,filetype) :
        """
        Arguments
        filetype -- the int id of the filename
        returns an list of rules
        """
        fileId = self.getFileId(filetype)
        if(fileId is None) :
            raise ValueError("Filetype does not exist")
        return self.session.query(Rule).options(subqueryload("rule_type")).options(subqueryload("file_column")).filter(FileColumn.file_column_id == fileId).all()
