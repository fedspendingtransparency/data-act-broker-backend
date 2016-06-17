from dataactcore.models.stagingInterface import StagingInterface
from dataactcore.models.stagingModels import Appropriation, ObjectClassProgramActivity, AwardFinancial, AwardFinancialAssistance
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

class ValidatorStagingInterface(StagingInterface):
    """ Manages all interaction with the staging database """
    MODEL_MAP = {"award":AwardFinancialAssistance,"award_financial":AwardFinancial,"appropriations":Appropriation,"program_activity":ObjectClassProgramActivity}

    def insertSubmissionRecordByFileType(self, record, fileType):
        """Insert a submitted file row into staging.

        Args:
            record: record to insert (Dict)
            submissionId: submissionId associated with this record (int)
            fileType: originating file type of the record (string)
        """

        rec = self.getModel(fileType)(**record)
        self.session.add(rec)
        self.session.commit()

    def getModel(self,fileType):
        """ Get model object for specified file type """
        if fileType not in self.MODEL_MAP:
            raise ResponseException("Not a valid file type: {}".format(fileType),StatusCode.INTERNAL_ERROR,KeyError)
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
            submission_id = submissionId
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

    def getNumberOfValidRecordsForSubmission(self,submissionId, fileType):
        """ Get number of valid records for submission """
        model = self.getModel(fileType)
        # Get number of valid records
        return self.session.query(model).filter(model.submission_id == submissionId).filter(model.valid_record == True).count()
