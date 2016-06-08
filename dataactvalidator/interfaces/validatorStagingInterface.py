from dataactcore.models.stagingInterface import StagingInterface
from dataactcore.models.stagingModels import Appropriation, ObjectClassProgramActivity, AwardFinancial, AwardFinancialAssistance


class ValidatorStagingInterface(StagingInterface):
    """ Manages all interaction with the staging database """

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
