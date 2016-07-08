from sqlalchemy.orm import subqueryload, joinedload
from sqlalchemy.orm.exc import NoResultFound
from dataactcore.models.validationInterface import ValidationInterface
from dataactcore.models.validationModels import Rule, RuleType, FileColumn, FileType, FieldType, RuleTiming, RuleSeverity, RuleSql
from dataactcore.models.domainModels import TASLookup
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactcore.config import CONFIG_DB


class ValidatorValidationInterface(ValidationInterface):
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

    def addColumnByFileType(self, fileType, fieldName, fieldNameShort, required, field_type, paddedFlag = "False"):
        """
        Adds a new column to the schema

        Args:
        fileType -- One of the set of valid types of files (e.g. Award, AwardFinancial)

        fieldName -- The name of the schema column
        fieldNameShort -- The machine-friendly, short column name
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
        newColumn.name_short = fieldNameShort
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

        # Translate padded flag to true or false
        if not paddedFlag:
            newColumn.padded_flag = False
        elif paddedFlag.lower() == "true":
            newColumn.padded_flag = True
        else:
            newColumn.padded_flag = False

        #Check types
        if field_type in types :
            newColumn.field_types_id =  types[field_type]
        else :
            raise ValueError("".join(["Type ",field_type," is not valid for  ",str(fieldName)]))
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
            self.session.query(Rule).filter(Rule.file_id == fileId).delete(synchronize_session="fetch")

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

    def getRulesByFile(self, fileType, ruleTiming = None) :
        """
        Arguments
        fileType -- the int id of the filename
        returns an list of rules
        """
        fileId = self.getFileId(fileType)
        if(fileId is None) :
            raise ValueError("Filetype does not exist")
        query = self.session.query(Rule).options(joinedload("rule_type")).options(joinedload("file_column")).filter(FileColumn.file_id == fileId)
        if ruleTiming:
            ruleTimingId = self.getRuleTimingIdByName(ruleTiming)
            query = query.filter(Rule.rule_timing_id == ruleTimingId)
        rules = query.all()
        return rules

    def addRule(self, columnId, ruleTypeText, ruleTextOne, ruleTextTwo, description, rule_timing = 1, rule_label = None, targetFileId = None, fileId = None, originalLabel = None):
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
        newRule = Rule(file_column_id = columnId, rule_type_id = self.getRuleType(ruleTypeText), rule_text_1 = ruleTextOne, rule_text_2 = ruleTextTwo,
                       description = description, rule_timing_id = rule_timing, rule_label = rule_label, target_file_id = targetFileId, file_id = fileId, original_label = originalLabel)
        self.session.add(newRule)
        self.session.commit()
        return True

    def addSqlRule(self, ruleSql, ruleLabel, ruleDescription, ruleErrorMsg,
        fileId, ruleSeverity, crossFileFlag=False, queryName = None, targetFileId = None):
        """Insert SQL-based validation rule.

        Args:
            ruleSql = the validation expressed as SQL
            ruleLabel = rule label used in the DATA Act standard
            ruleDescription = rule description
            ruleErrorMsg = the rule's standard error message
            crossFileFlag = indicates whether or not the rule is cross-file
            fileType = file type that the rule applies to
            severityName = severity of the rule

        Returns:
            ID of new rule
        """
        newRule = RuleSql(rule_sql=ruleSql, rule_label=ruleLabel,
                rule_description=ruleDescription, rule_error_message=ruleErrorMsg,
                rule_cross_file_flag=crossFileFlag, file_id=fileId, rule_severity=ruleSeverity, query_name = queryName, target_file_id = targetFileId)
        self.session.add(newRule)
        self.session.commit()
        return True

    def deleteSqlRules(self):
        """Delete existing rules in the SQL rules table."""
        self.session.query(RuleSql).delete()
        self.session.commit()
        return True

    def getMultiFieldRulesByFile(self, fileType):
        """ Uses rule_timing to specify multi field rules

        Args:
            fileType:  Which type of file to get rules for

        Returns:
            list of Rule objects
        """
        fileId = self.getFileId(fileType)
        return self.session.query(Rule).filter(Rule.file_id == fileId).filter(Rule.rule_timing_id == self.getRuleTimingIdByName("multi_field")).all()

    def getRulesByTiming(self, timing):
        """

        Args:
            timing: Which timing to get rules for

        Returns:
            list of Rule objects
        """
        timingId = self.getRuleTimingIdByName(timing)
        return self.session.query(Rule).filter(Rule.rule_timing_id == timingId).all()

    def getColumnById(self, file_column_id):
        """ Get File Column object from ID """
        return self.session.query(FileColumn).filter(FileColumn.file_column_id == file_column_id).first()

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

    def getColumn(self, fieldName, fileType):
        """ Find file column given field name and file type

        Args:
            fieldName: Field to search for
            fileId: Which file this field is associated with

        Returns:
            Column object for file column if found, otherwise raises exception
        """
        fileId = self.getFileId(fileType)
        column = self.session.query(FileColumn).filter(FileColumn.name == fieldName.lower()).filter(
            FileColumn.file_id == fileId)
        return self.runUniqueQuery(column, "No field found with that name for that file type",
                                   "Multiple fields with that name for that file type")

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


    def getRuleTypeById(self,typeId):
        """ Get rule name for specified id

        Args:
            typeId: rule_type_id

        Returns:
            Name of rule type
        """
        # Populate rule type dict
        return self.getNameFromDict(RuleType, "TYPE_DICT", "name", typeId, "rule_type_id")

    def populateFile(self,column):
        """ Populate file object in the ORM for the specified FileColumn object

        Arguments:
            column - FileColumn object to get File object for
        """
        column.file = self.session.query(FileType).filter(FileType.file_id == column.file_id)[0]

    def getRuleTimingIdByName(self,timingName):
        """ Get rule timing ID for specified rule timing

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

    def getRuleSeverityByName(self, ruleSeverityName):
        """Return rule severity based on name."""
        query = self.session.query(RuleSeverity).filter(RuleSeverity.name == ruleSeverityName)
        query = self.runUniqueQuery(query, "No rule severity found with name {}".format(ruleSeverityName),
            "Multiple rule severities found with name {}".format(ruleSeverityName))
        return query

    def getLongToShortColname(self):
        """Return a dictionary that maps schema field names to shorter, machine-friendly versions."""
        query = self.session.query(FileColumn.name, FileColumn.name_short).all()
        dict = {row.name:row.name_short for row in query}
        return dict

    def getShortToLongColname(self):
        """Return a dictionary that maps short, machine-friendly schema names to their long versions."""
        query = self.session.query(FileColumn.name, FileColumn.name_short).all()
        dict = {row.name_short: row.name for row in query}
        return dict

    def isPadded(self, field, fileType):
        """ Returns padded_flag for specified field and filetype """
        column = self.getColumn(field, fileType)
        return column.padded_flag
