from sqlalchemy.orm import subqueryload
from dataactcore.models import validationInterface
from dataactcore.models.validationModels import TASLookup, Rule, RuleType, FileColumn, FileType ,FieldType, MultiFieldRule, MultiFieldRuleType
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner

class ValidatorValidationInterface(validationInterface.ValidationInterface) :
    """ Manages all interaction with the validation database """


    def deleteTAS(self) :
        """
        Removes the TAS table
        """
        queryResult = self.session.query(TASLookup).delete(synchronize_session='fetch')
        self.session.commit()


    def addTAS(self,ata,aid,bpoa,epoa,availability,main,sub):
        """

        Add a TAS to the validation database if it does not exist.
        This method can be slow.

        Args:
            ata -- allocation transfer agency
            aid --  agency identifier
            bpoa -- beginning period of availability
            epoa -- ending period of availability
            availability -- availability type code
            main --  main account code
            sub -- sub account code
        """
        queryResult = self.session.query(TASLookup).\
            filter(TASLookup.allocation_transfer_agency == ata).\
            filter(TASLookup.agency_identifier == aid).\
            filter(TASLookup.beginning_period_of_availability == bpoa).\
            filter(TASLookup.ending_period_of_availability == epoa).\
            filter(TASLookup.availability_type_code == availability).\
            filter(TASLookup.main_account_code == main).\
            filter(TASLookup.sub_account_code == sub).all()
        if ( len(queryResult) == 0) :
            tas = TASLookup()
            tas.allocation_transfer_agency =ata
            tas.agency_identifier=aid
            tas.beginning_period_of_availability = bpoa
            tas.ending_period_of_availability = epoa
            tas.availability_type_code = availability
            tas.main_account_code = main
            tas.sub_account_code = sub
            self.session.add(tas)
            self.session.commit()
            return True
        return False

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
            raise ValueError("".join(["Type ",field_type," is not vaild for  ",str(fieldName)]))
        #Check Required
        required = required.upper()
        if( required in ["TRUE","FALSE"]) :
            if( required == "TRUE") :
                newColumn.required = True
        else :
            raise ValueError("".join(["Required is not boolean for ",str(fieldName)]))
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
        # Get set of file columns for this file
        #columns = self.session.query(FileColumn).filter(FileColumn.file_id == fileId).all()
        # Delete all rules for those columns
        #self.session.query(Rule).filter(Rule.file_column_id.in_(columns)).delete()
        self.session.execute("DELETE FROM rule where file_column_id in (SELECT file_column_id FROM file_columns WHERE file_id = :param)",{"param":fileId})
        self.session.execute("DELETE FROM multi_field_rule WHERE file_id = :param",{"param":fileId})
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
        for result in queryResult:
            result.name = FieldCleaner.cleanString(result.name) # Standardize field names
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
            returnDict[column.name.lower().replace(" ","_")]  = column
        return returnDict


    def getFileId(self, filename) :
        """ Retrieves ID for specified file type

        Args:
            filename: Type of file to get ID for

        Returns:
            ID if file type found, or None if file type is not found
        """
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
        return True

    def addMultiFieldRule(self,fileId, ruleTypeText, ruleTextOne, ruleTextTwo, description):
        """

        Args:
            fileId:  Which file this rule applies to
            ruleTypeText: type for this rule
            ruleTextOne: definition of rule
            ruleTextTwo: definition of rule
            description: readable explanation of rule

        Returns:
            True if successful
        """
        newRule = MultiFieldRule(file_id = fileId, multi_field_rule_type_id = MultiFieldRuleType.getType(ruleTypeText), rule_text_1 = ruleTextOne, rule_text_2 = ruleTextTwo, description = description)
        self.session.add(newRule)
        self.session.commit()
        return True

    def getMultiFieldRulesByFile(self, fileType):
        """

        Args:
            fileType:  Which type of file to get rules for

        Returns:
            list of MultiFieldRule objects
        """
        fileId = self.getFileId(fileType)
        return self.session.query(MultiFieldRule).filter(MultiFieldRule.file_id == fileId).all()

    def getColumnId(self, fieldName, fileId):
        """ Find file column given field name and file type

        Args:
            fieldName: Field to search for
            fileId: Which file this field is associated with

        Returns:
            ID for file column if found, otherwise raises exception
        """
        column = self.session.query(FileColumn).filter(FileColumn.name == fieldName.lower()).filter(FileColumn.file_id == fileId).all()
        self.checkUnique(column,"No field found with that name for that file type", "Multiple fields with that name for that file type")
        return column[0].file_column_id
