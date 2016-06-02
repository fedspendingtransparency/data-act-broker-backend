from sqlalchemy.orm import subqueryload, joinedload
from sqlalchemy.orm.exc import NoResultFound
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.validationModels import TASLookup, Rule, RuleType, FileColumn, FileType, FieldType, MultiFieldRule, MultiFieldRuleType, RuleTiming
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactcore.config import CONFIG_DB


class ValidatorValidationInterface(BaseInterface):
    """ Manages all interaction with the validation database """

    dbConfig = CONFIG_DB
    dbName = dbConfig['validator_db_name']
    Session = None
    engine = None
    session = None

    def __init__(self):
        self.dbName = self.dbConfig['validator_db_name']
        super(ValidatorValidationInterface, self).__init__()

    @classmethod
    def getCredDict(cls):
        """ Return db credentials. """
        credDict = {
            'username': CONFIG_DB['username'],
            'password': CONFIG_DB['password'],
            'host': CONFIG_DB['host'],
            'port': CONFIG_DB['port'],
            'dbBaseName': CONFIG_DB['base_db_name']
        }
        return credDict

    @staticmethod
    def getDbName():
        """ Return database name"""
        return ValidatorValidationInterface.dbName

    def getSession(self):
        """ Return current session object """
        return self.session

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
        columns = self.session.query(FileColumn).filter(FileColumn.file_id == fileId).all()
        # Get list of ids for file columns
        columnIds = []
        for column in columns:
            columnIds.append(column.file_column_id)
        if(len(columnIds) > 0):
            # Delete all rules for those columns
            self.session.query(Rule).filter(Rule.file_column_id.in_(columnIds)).delete(synchronize_session="fetch")
            # Delete multi field rules
            self.session.query(MultiFieldRule).filter(MultiFieldRule.file_id == fileId).delete(synchronize_session="fetch")

        self.session.commit()
        #raise Exception("Check table, rules removed for file " + str(fileId))

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
            returnDict[FieldCleaner.cleanString(column.name)]  = column
        return returnDict


    def getFileId(self, filename) :
        """ Retrieves ID for specified file type

        Args:
            filename: Type of file to get ID for

        Returns:
            ID if file type found, or None if file type is not found
        """
        query = self.session.query(FileType).filter(FileType.name== filename)
        return self.runUniqueQuery(query,"No ID for specified file type","Conflicting IDs for specified file type").file_id

    def getRulesByFile(self, fileType) :
        """
        Arguments
        fileType -- the int id of the filename
        returns an list of rules
        """
        fileId = self.getFileId(fileType)
        if(fileId is None) :
            raise ValueError("Filetype does not exist")
        rules = self.session.query(Rule).options(joinedload("rule_type")).options(joinedload("file_column")).filter(FileColumn.file_id == fileId).all()
        return rules

    def addRule(self, columnId, ruleTypeText, ruleText, description, rule_timing = 1, rule_label = None):
        """

        Args:
            columnId: ID of column to add rule for
            ruleTypeText: Specifies which type of rule by one of the names in the rule_type table
            ruleText: Usually a number to compare to, e.g. length or value to be equal to

        Returns:
            True if successful
        """
        if rule_timing is None or rule_timing == "":
            # Use default value if timing is unspecified
            rule_timing = 1
        newRule = Rule(file_column_id = columnId, rule_type_id = self.getRuleType(ruleTypeText), rule_text_1 = ruleText,
                       description = description, rule_timing_id = rule_timing, rule_label = rule_label)
        self.session.add(newRule)
        self.session.commit()
        return True

    def addMultiFieldRule(self, fileId, ruleTypeText, ruleTextOne, ruleTextTwo, description, ruleLabel=None, ruleTiming=1, targetFileId=None):
        """

        Args:
            fileId:  Which file this rule applies to
            ruleTypeText: type for this rule
            ruleTextOne: definition of rule
            ruleTextTwo: definition of rule
            description: readable explanation of rule
            ruleLabel: a label used to refer to the rule
            targetFileId: the file this rule validates against (applicable only for certain cross-file rules)
            ruleTiming: rule timing id

        Returns:
            True if successful
        """
        newRule = MultiFieldRule(file_id=fileId, multi_field_rule_type_id=self.getMultiFieldRuleType(ruleTypeText),
                                 rule_text_1=ruleTextOne, rule_text_2=ruleTextTwo, description=description,
                                 rule_label=ruleLabel, rule_timing_id=ruleTiming, target_file_id=targetFileId)
        self.session.add(newRule)
        self.session.commit()
        return True

    def getMultiFieldRuleByLabel(self, label):
        """ Find multi field rule by label """
        ruleQuery = self.session.query(MultiFieldRule).filter(MultiFieldRule.rule_label == label)
        return self.runUniqueQuery(ruleQuery,"Rule label not found", "Multiple rules match specified label")

    def getMultiFieldRulesByFile(self, fileType):
        """

        Args:
            fileType:  Which type of file to get rules for

        Returns:
            list of MultiFieldRule objects
        """
        fileId = self.getFileId(fileType)
        return self.session.query(MultiFieldRule).filter(MultiFieldRule.file_id == fileId).filter(MultiFieldRule.rule_timing_id == self.getRuleTimingIdByName("file_validation")).all()

    def getMultiFieldRulesByTiming(self, timing):
        """

        Args:
            fileType:  Which type of file to get rules for

        Returns:
            list of MultiFieldRule objects
        """
        timingId = self.getRuleTimingIdByName(timing)
        return self.session.query(MultiFieldRule).filter(MultiFieldRule.rule_timing_id == timingId).all()

    def getColumnId(self, fieldName, fileType):
        """ Find file column given field name and file type

        Args:
            fieldName: Field to search for
            fileId: Which file this field is associated with

        Returns:
            ID for file column if found, otherwise raises exception
        """
        fileId = self.getFileId(fileType)
        column = self.session.query(FileColumn).filter(FileColumn.name == fieldName.lower()).filter(FileColumn.file_id == fileId)
        return self.runUniqueQuery(column,"No field found with that name for that file type", "Multiple fields with that name for that file type").file_column_id

    def getColumnLength(self,fieldName, fileId):
        """ If there is a length rule for this field, return the max length.  Otherwise, return None. """
        columnId = self.getColumnId(fieldName,fileId)
        # Get length rules for this column
        query = self.session.query(Rule).filter(Rule.file_column_id == columnId).filter(Rule.rule_type_id == 6)
        try:
            rule = self.runUniqueQuery(query,False,"Multiple length rules for this column")
        except NoResultFound as e:
            # No length rule for this column
            return None
        return int(float(rule.rule_text_1)) # Going through float in case of decimal value

    def getRuleType(self,typeName):
        """ Get rule ID for specified rule type

        Arguments:
            typeName - name of rule type (string)
        Returns:
            ID for rule type (int)
        """
        return self.getIdFromDict(RuleType,"TYPE_DICT","name",typeName.upper(),"rule_type_id")

    def getMultiFieldRuleType(self,typeName):
        """ Get rule ID for specified multi-field rule type

        Arguments:
            typeName - name of rule type (string)
        Returns:
            ID for rule type (int)
        """
        return self.getIdFromDict(MultiFieldRuleType,"TYPE_DICT","name",typeName.upper(),"multi_field_rule_type_id")

    def getMultiFieldRuleTypeById(self,typeId):
        """ Get rule name for specified id

        Args:
            typeId: multi_field_rule_type_id

        Returns:
            Name of multi filed rule type
        """
        # Populate rule type dict
        return self.getNameFromDict(MultiFieldRuleType, "TYPE_DICT", "name", typeId, "multi_field_rule_type_id")

    def populateFile(self,column):
        """ Populate file object in the ORM for the specified FileColumn object

        Arguments:
            column - FileColumn object to get File object for
        """
        column.file = self.session.query(FileType).filter(FileType.file_id == column.file_id)[0]

    def getRuleTimingIdByName(self,timingName):
        """ Get rule ID for specified multi-field rule type

        Arguments:
            typeName - name of rule type (string)
        Returns:
            ID for rule type (int)
        """
        return self.getIdFromDict(RuleTiming,"TIMING_DICT","name",timingName.lower(),"rule_timing_id")

    def getRuleByLabel(self,label):
        """ Find rule based on label provided in rules file """
        query = self.session.query(Rule).options(joinedload("file_column")).filter(Rule.rule_label == label)
        return self.runUniqueQuery(query,"No rule with that label","Multiple rules have that label")

    def getFieldTypeById(self, id):
        """ Return name of field type based on id """
        return self.getNameFromDict(FieldType,"TYPE_DICT","name",id,"field_type_id")

    def getFieldNameByColId(self, id):
        """ Return field name based on a column ID.  Used to map staging database columns to matching field names. """
        int(id) # Raise appropriate error if id is not an int
        query = self.session.query(FileColumn).filter(FileColumn.file_column_id == id)
        column = self.runUniqueQuery(query,"No column found with that ID", "Multiple columns found with that ID")
        return column.name