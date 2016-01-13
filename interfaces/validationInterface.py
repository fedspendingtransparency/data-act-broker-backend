from dataactcore.models import validationInterface
from sqlalchemy.orm import subqueryload
from dataactcore.models.validationModels import Rule, RuleType, FileColumn, FileType ,FieldType

class ValidationInterface(validationInterface.ValidationInterface) :
    """ Manages all interaction with the validation database
    """

    def addColumnByFileType(self,fileType,fieldName,required,field_type):
        """
        Adds a new column to the schema

        Args:
        fileType -- One of the set of valid types of files (e.g. Award, AwardFinancial)

        fieldName -- The name of the scheam column
        required --  marks the column if data is allways required
        field_type  -- sets the type of data allowed in the column

        Returns:
            ID of new column
        """
        fileId = self.getFileId(fileType)
        if(fileId is None) :
            raise ValueError("Filetype does not exist")
        newColumn = FileColumn()
        newColumn.required = False
        newColumn.name = fieldName
        newColumn.file_id = fileId
        field_type = field_type.upper()

        types = self.getDataTypes()
        #Allow for other names
        if(field_type == "STR") :
            field_type = "STRING"
        elif(field_type  == "FLOAT") :
            field_type = "DECIMAL"
        elif(field_type  == "BOOL"):
            field_type = "BOOLEAN"

        #Check types
        if field_type in types :
            newColumn.field_types_id =  types[field_type]
        else :
            raise ValueError("Type " +field_type + " is not vaild for  " + str(fieldName) )
        #Check Required
        required = required.upper()
        if( required in ["TRUE","FALSE"]) :
            if( required == "TRUE") :
                newColumn.required = True
        else :
            raise ValueError("Required is not boolean for " + str(fieldName) )
        # Save
        self.session.add(newColumn)
        self.session.commit()
        return newColumn.file_column_id

    def getDataTypes(self) :
        """"
        Returns a dictionary of data types that contains the id of the types
        """
        dataTypes  = {}
        queryResult = self.session.query(FieldType).all()
        for column in queryResult :
            dataTypes[column.name] = column.field_type_id
        return dataTypes

    def removeColumnsByFileType(self,fileType) :
        """
        Removes the schema for a file

        Args:
        fileType -- One of the set of valid types of files (e.g. Award, AwardFinancial)
        """
        fileId = self.getFileId(fileType)
        if(fileId is None) :
            raise ValueError("Filetype does not exist")
        queryResult = self.session.query(FileColumn).filter(FileColumn.file_id == fileId).delete(synchronize_session='fetch')
        self.session.commit()

    def removeRulesByFileType(self,fileType) :
        """
        Removes the rules for a file

        Args:
        fileType -- One of the set of valid types of files (e.g. Award, AwardFinancial)
        """
        fileId = self.getFileId(fileType)
        if(fileId is None) :
            raise ValueError("Filetype does not exist")
        self.session.execute("DELETE FROM rule where file_column_id in (SELECT file_column_id FROM file_columns WHERE file_id = :param)",{"param":fileId})
        self.session.commit()

    def getFieldsByFileList(self, fileType):
        """ Returns a list of valid field names that can appear in this type of file

        Args:
        fileType -- One of the set of valid types of files (e.g. Award, AwardFinancial)

        Returns:
        list of names
        """
        fileId = self.getFileId(fileType)
        returnList  = []
        if(fileId is None) :
            raise ValueError("Filetype does not exist")
        queryResult = self.session.query(FileColumn).filter(FileColumn.file_id == fileId).all()
        return queryResult

    def getFieldsByFile(self, fileType):
        """ Returns a dict of valid field names that can appear in this type of file

        Args:
        fileType -- One of the set of valid types of files (e.g. Award, AwardFinancial)

        Returns:
        dict with field names as keys and values are ORM object FileColumn
        """
        returnDict = {}
        fileId = self.getFileId(fileType)
        if(fileId is None) :
            raise ValueError("File type does not exist")
        queryResult = self.session.query(FileColumn).options(subqueryload("field_type")).filter(FileColumn.file_id == fileId).all()
        for column in queryResult :
            returnDict[column.name]  = column
        return returnDict


    def getFileId(self, filename) :
        queryResult = self.session.query(FileType).filter(FileType.name== filename).all()
        if(len(queryResult) > 0) :
            return queryResult[0].file_id
        return None

    def getRulesByFile(self, fileType) :
        """
        Arguments
        fileType -- the int id of the filename
        returns an list of rules
        """
        fileId = self.getFileId(fileType)
        if(fileId is None) :
            raise ValueError("Filetype does not exist")
        return self.session.query(Rule).options(subqueryload("rule_type")).options(subqueryload("file_column")).filter(FileColumn.file_id == fileId).all()

    def addRule(self, columnId, ruleTypeText, ruleText, description):
        """

        Args:
            columnId: ID of column to add rule for
            ruleTypeText: Specifies which type of rule by one of the names in the rule_type table
            ruleText: Usually a number to compare to, e.g. length or value to be equal to

        Returns:
            True if successful
        """
        newRule = Rule(file_column_id = columnId, rule_type_id = RuleType.getType(ruleTypeText), rule_text_1 = ruleText, description = description)
        self.session.add(newRule)
        self.session.commit()