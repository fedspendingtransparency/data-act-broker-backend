from dataactcore.models.stagingInterface import StagingInterface
from dataactcore.models.stagingModels import FieldNameMap
from dataactcore.models.stagingModels import Appropriation, ObjectClassProgramActivity, AwardFinancial, AwardFinancialAssistance
from sqlalchemy import MetaData, Table
from sqlalchemy.exc import NoSuchTableError


class ValidatorStagingInterface(StagingInterface):
    """ Manages all interaction with the staging database """

    def dropTable(self,table):
        """

        Args:
            table: Table to be dropped

        Returns:
            True if successful
        """

        metadata = MetaData()
        stagingTable = Table(table, metadata, autoload_with=self.engine)
        stagingTable.drop(bind=self.engine)

    def tableExists(self,table):
        """ True if table exists, false otherwise """
        return self.engine.dialect.has_table(self.engine.connect(),table)

    def countRows(self,table):
        """ Returns number of rows in the specified table """
        metadata = MetaData()
        try:
            stagingTable = Table(table, metadata, autoload_with=self.engine)
        except NoSuchTableError:
            return 0
        rows = self.session.query(stagingTable).count()
        self.session.close()
        return rows

    def getFieldNameMap(self, tableName):
        """ Return the dict mapping column IDs to field names """
        query = self.session.query(FieldNameMap).filter(FieldNameMap.table_name == tableName)
        return self.runUniqueQuery(query,"No map for that table", "Conflicting maps for that table").column_to_field_map

    def addFieldNameMap(self, tableName, fieldNameMap):
        """ Add dict for field names to staging DB

        Args:
            tableName: Table map is being added for
            fieldNameMap: Dict with column IDs as keys and field names as values
        """
        newMap = FieldNameMap(table_name = tableName, column_to_field_map = str(fieldNameMap))
        self.session.add(newMap)
        self.session.commit()

    def insertSubmissionRecordByFileType(self, record, fileType):
        """Insert a submitted file row into staging.

        Args:
            record: record to insert (Dict)
            submissionId: submissionId associated with this record (int)
            fileType: originating file type of the record (string)
        """
        if fileType == "award":
            rec = AwardFinancialAssistance(**record)
        elif fileType == "award_financial":
            rec = AwardFinancial(**record)
        elif fileType == "appropriations":
            rec = Appropriation(**record)
        elif fileType == "program_activity":
            rec = ObjectClassProgramActivity(**record)
        self.session.add(rec)
        self.session.commit()

    def getSubmissionRecordsByFileType(self, submissionId, fileType):
        """Return records for a specific submission and file type.

        Args:
            submissionId: the submission to retrieve records for (int)
            fileType: the file type to pull (string)

        Returns:
            Query
        """
        if fileType == "award":
            recs = self.session.query(AwardFinancialAssistance).filter_by(
                submission_id=submissionId)
        elif fileType == "award_financial":
            recs = self.session.query(AwardFinancial).filter_by(
                submission_id=submissionId)
        elif fileType == "appropriations":
            recs = self.session.query(Appropriation).filter_by(
                submission_id=submissionId)
        elif fileType == "program_activity":
            recs = self.session.query(ObjectClassProgramActivity).filter_by(
                submission_id=submissionId)
        else:
            raise ValueError(
                "No staging table found for fileType {}".format(fileType))
        return recs
