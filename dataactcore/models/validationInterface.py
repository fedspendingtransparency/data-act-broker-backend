from sqlalchemy.orm import subqueryload

from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.domainModels import CGAC, TASLookup
from dataactcore.models.validationModels import FileColumn, FileTypeValidation, FieldType, RuleSeverity, RuleSql
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

    def getAllAgencies(self):
        """ Return all agencies """
        return self.session.query(CGAC).all()

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

    def getCGACCode(self, agency_name):
        query = self.session.query(CGAC).filter(CGAC.agency_name == agency_name)
        result = self.runUniqueQuery(query, "No CGAC Code found for specified agency name", "Multiple CGAC codes found for specified agency name")
        return result.cgac_code

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

    def getSubmissionsByFileType(self, submissionId, fileType):
        """Return records for a specific submission and file type.

        Args:
            submissionId: the submission to retrieve records for (int)
            fileType: the file type to pull (string)

        Returns:
            Query
        """
        return self.session.query(self.getModel(fileType)).filter_by(
            submission_id=submissionId
        )

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

    def addTAS(self, ata, aid, bpoa, epoa, availability, main, sub):
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
        queryResult = self.session.query(TASLookup). \
            filter(TASLookup.allocation_transfer_agency == ata). \
            filter(TASLookup.agency_identifier == aid). \
            filter(TASLookup.beginning_period_of_availability == bpoa). \
            filter(TASLookup.ending_period_of_availability == epoa). \
            filter(TASLookup.availability_type_code == availability). \
            filter(TASLookup.main_account_code == main). \
            filter(TASLookup.sub_account_code == sub).all()
        if (len(queryResult) == 0):
            tas = TASLookup()
            tas.allocation_transfer_agency = ata
            tas.agency_identifier = aid
            tas.beginning_period_of_availability = bpoa
            tas.ending_period_of_availability = epoa
            tas.availability_type_code = availability
            tas.main_account_code = main
            tas.sub_account_code = sub
            self.session.add(tas)
            self.session.commit()
            return True
        return False

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

    def addSqlRule(self, ruleSql, ruleLabel, ruleDescription, ruleErrorMsg,
                   fileId, ruleSeverity, crossFileFlag=False, queryName=None, targetFileId=None):
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
                          rule_cross_file_flag=crossFileFlag, file_id=fileId, rule_severity=ruleSeverity,
                          query_name=queryName, target_file_id=targetFileId)
        self.session.add(newRule)
        self.session.commit()
        return True

    def deleteSqlRules(self):
        """Delete existing rules in the SQL rules table."""
        self.session.query(RuleSql).delete()
        self.session.commit()
        return True

    def getColumnById(self, file_column_id):
        """ Get File Column object from ID """
        return self.session.query(FileColumn).filter(FileColumn.file_column_id == file_column_id).first()

    def getColumnId(self, fieldName, fileType, shortCols=True):
        """ Find file column given field name and file type

        Args:
            fieldName: Field to search for
            fileId: Which file this field is associated with
            shortCols: If true, search by short col names

        Returns:
            ID for file column if found, otherwise raises exception
        """
        fileId = self.getFileTypeIdByName(fileType)
        if shortCols:
            column = self.session.query(FileColumn).filter(
                FileColumn.name_short == fieldName.lower()).filter(
                FileColumn.file_id == fileId)
        else:
            column = self.session.query(FileColumn).filter(
                FileColumn.name == fieldName.lower()).filter(
                FileColumn.file_id == fileId)
        return self.runUniqueQuery(column,
                                   "No field found with that name for that file type",
                                   "Multiple fields with that name for that file type").file_column_id

    def getColumn(self, fieldName, fileType):
        """ Find file column given field name and file type

        Args:
            fieldName: Field to search for
            fileId: Which file this field is associated with

        Returns:
            Column object for file column if found, otherwise raises exception
        """
        fileId = self.getFileTypeIdByName(fileType)
        column = self.session.query(FileColumn).filter(FileColumn.name_short == fieldName.lower()).filter(
            FileColumn.file_id == fileId)
        return self.runUniqueQuery(column, "No field found with that name for that file type",
                                   "Multiple fields with that name for that file type")

    def populateFile(self, column):
        """ Populate file object in the ORM for the specified FileColumn object

        Arguments:
            column - FileColumn object to get File object for
        """
        column.file = self.session.query(FileTypeValidation).filter(FileTypeValidation.file_id == column.file_id)[0]

    def getFieldTypeById(self, id):
        """ Return name of field type based on id """
        return self.getNameFromDict(FieldType, "TYPE_DICT", "name", id, "field_type_id")

    def getRuleSeverityByName(self, ruleSeverityName):
        """Return rule severity based on name."""
        query = self.session.query(RuleSeverity).filter(RuleSeverity.name == ruleSeverityName)
        query = self.runUniqueQuery(query, "No rule severity found with name {}".format(ruleSeverityName),
                                    "Multiple rule severities found with name {}".format(ruleSeverityName))
        return query

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

    def isPadded(self, field, fileType):
        """ Returns padded_flag for specified field and filetype """
        column = self.getColumn(field, fileType)
        return column.padded_flag
