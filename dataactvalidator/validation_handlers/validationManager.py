import os
import traceback
import sys
from csv import Error
from sqlalchemy import or_, and_
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.validationModels import FileTypeValidation
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.cloudLogger import CloudLogger
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactvalidator.filestreaming.csvS3Reader import CsvS3Reader
from dataactvalidator.filestreaming.csvLocalReader import CsvLocalReader
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer
from dataactvalidator.validation_handlers.validator import Validator
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactcore.models.validationModels import RuleSql


class ValidationManager:
    """
    Outer level class, called by flask route
    """
    reportHeaders = ["Field name", "Error message", "Row number", "Value provided", "Rule label"]
    crossFileReportHeaders = ["Source File", "Target File", "Field names", "Error message", "Values provided", "Row number", "Rule label"]

    def __init__(self,isLocal =True,directory=""):
        # Initialize instance variables
        self.filename = ""
        self.isLocal = isLocal
        self.directory = directory

    @staticmethod
    def markJob(jobId,jobTracker,status,errorDb,filename=None, fileError = ValidationError.unknownError, extraInfo = None):
        """ Update status of a job in job tracker database
        Args:
            jobId: Job to be updated
            jobTracker: Interface object for job tracker
            status: New status for specified job
            errorDb: Interface object for error database
            filename: Filename of file to be validated
            fileError: Type of error that occurred if this is an invalid or failed status
            extraInfo: Dict of extra fields to attach to exception
        """
        try:
            if filename != None and (status == "invalid" or status == "failed"):
                # Mark the file error that occurred
                errorDb.writeFileError(jobId, filename, fileError, extraInfo)
            jobTracker.markJobStatus(jobId, status)
        except ResponseException as e:
            # Could not get a unique job ID in the database, either a bad job ID was passed in
            # or the record of that job was lost.
            # Either way, cannot mark status of a job that does not exist
            open("databaseErrors.log", "a").write("".join(
                ["Could not mark status ", str(status), " for job ID ", str(jobId), "\n"]))

    @staticmethod
    def getJobID(request):
        """ Pull job ID out of request
        Args:
            request: HTTP request containing the job ID
        Returns:
            job ID, or raises exception if job ID not found in request
        """
        requestDict = RequestDictionary(request)
        if requestDict.exists("job_id"):
            jobId = requestDict.getValue("job_id")
            return jobId
        else:
            # Request does not have a job ID, can't validate
            raise ResponseException("No job ID specified in request", StatusCode.CLIENT_ERROR)

    @staticmethod
    def testJobID(jobId, interfaces):
        """
        args:
            jobId: job to be tested
            interfaces: InterfaceHolder to the databases

        returns:
            True if the job is ready, if the job is not ready an exception will be raised
        """
        if not interfaces.jobDb.runChecks(jobId):
            raise ResponseException("Checks failed on Job ID", StatusCode.CLIENT_ERROR)

        return True

    def threadedValidateJob(self, jobId):
        """
        args
        jobId -- (Integer) a valid jobId
        This method runs on a new thread thus
        there are zero error messages other then the
        job status being updated
        """

        # As this is the start of a new thread, first generate new connections to the databases
        BaseInterface.interfaces = None
        interfaces = InterfaceHolder()

        self.filename = ""
        jobTracker = interfaces.jobDb
        errorDb = interfaces.errorDb
        try:
            jobType = interfaces.jobDb.checkJobType(jobId)
            if jobType == interfaces.jobDb.getJobTypeId("csv_record_validation"):
                self.runValidation(jobId, interfaces)
            elif jobType == interfaces.jobDb.getJobTypeId("validation"):
                self.runCrossValidation(jobId, interfaces)
            else:
                raise ResponseException("Bad job type for validator",
                                        StatusCode.INTERNAL_ERROR)
            self.runValidation(jobId, interfaces)
            errorDb.markFileComplete(jobId, self.filename)
            return
        except ResponseException as e:
            CloudLogger.logError(str(e), e, traceback.extract_tb(sys.exc_info()[2]))
            self.markJob(jobId, jobTracker, "invalid", errorDb, self.filename,
                         e.errorType, e.extraInfo)
        except ValueError as e:
            CloudLogger.logError(str(e), e, traceback.extract_tb(sys.exc_info()[2]))
            self.markJob(jobId, jobTracker, "invalid", errorDb, self.filename,
                         ValidationError.unknownError)
        except Exception as e:
            # Something unknown happened we may need to try again!
            CloudLogger.logError(str(e), e, traceback.extract_tb(sys.exc_info()[2]))
            self.markJob(jobId,jobTracker, "failed", errorDb, self.filename,
                         ValidationError.unknownError)
        finally:
            interfaces.close()

    def getReader(self):
        """
        Gets the reader type based on if its local install or not.
        """
        if self.isLocal:
            return CsvLocalReader()
        return CsvS3Reader()

    def getWriter(self, regionName, bucketName, fileName, header):
        """ Gets the write type based on if its a local install or not.

        Args:
            regionName - AWS region to write to, not used for local
            bucketName - AWS bucket to write to, not used for local
            fileName - File to be written
            header - Column headers for file to be written
        """
        if self.isLocal:
            return CsvLocalWriter(fileName, header)
        return CsvS3Writer(regionName, bucketName, fileName, header)

    def getFileName(self,path):
        """ Return full path of error report based on provided name """
        if self.isLocal:
            return os.path.join(self.directory, path)
        # Forcing forward slash here instead of using os.path to write a valid path for S3
        return "".join(["errors/", path])

    def readRecord(self,reader,writer,fileType,interfaces,rowNumber,jobId,isFirstQuarter, fields):
        """ Read and process the next record

        Args:
            reader: CsvReader object
            writer: CsvWriter object
            fileType: Type of file for current job
            interfaces: InterfaceHolder object
            rowNumber: Next row number to be read
            jobId: ID of current job
            isFirstQuarter: True if submission ends in first quarter

        Returns:
            Tuple with four elements:
            1. Dict of record after preprocessing
            2. Boolean indicating whether to reduce row count
            3. Boolean indicating whether to skip row
            4. Boolean indicating whether to stop reading
            5. Row error has been found
        """
        errorInterface = interfaces.errorDb
        reduceRow = False
        rowErrorFound = False
        try:

            record = FieldCleaner.cleanRow(reader.getNextRecord(), fileType, interfaces.validationDb, self.longToShortDict, fields)
            record["row_number"] = rowNumber
            record["is_first_quarter"] = isFirstQuarter
            if reader.isFinished and len(record) < 2:
                # This is the last line and is empty, don't record an error
                return {}, True, True, True, False  # Don't count this row
        except ResponseException as e:
            if reader.isFinished and reader.extraLine:
                #Last line may be blank don't record an error, reader.extraLine indicates a case where the last valid line has extra line breaks
                # Don't count last row if empty
                reduceRow = True
            else:
                writer.write(["Formatting Error", ValidationError.readErrorMsg, str(rowNumber), ""])
                errorInterface.recordRowError(jobId,self.filename,"Formatting Error",ValidationError.readError,rowNumber,severity_id=interfaces.validationDb.getRuleSeverityId("fatal"))
                rowErrorFound = True
            return {}, reduceRow, True, False, rowErrorFound
        return record, reduceRow, False, False, rowErrorFound

    def writeToStaging(self, record, jobId, submissionId, passedValidations, interfaces, writer, rowNumber, fileType):
        """ Write this record to the staging tables

        Args:
            record: Record to be written
            jobId: ID of current job
            submissionId: ID of current submission
            passedValidations: True if record has not failed first validations
            interfaces: InterfaceHolder object
            writer: CsvWriter object
            rowNumber: Current row number
            fileType: Type of file for current job

        Returns:
            Boolean indicating whether to skip current row
        """
        stagingInterface = interfaces.stagingDb
        errorInterface = interfaces.errorDb
        try:
            record["job_id"] = jobId
            record["submission_id"] = submissionId
            record["valid_record"] = passedValidations
            stagingInterface.insertSubmissionRecordByFileType(record, fileType)
        except ResponseException as e:
            # Write failed, move to next record
            writer.write(["Formatting Error", ValidationError.writeErrorMsg, str(rowNumber),""])
            errorInterface.recordRowError(jobId, self.filename,
                "Formatting Error",ValidationError.writeError, rowNumber,severity_id=interfaces.validationDb.getRuleSeverityId("fatal"))
            return True
        return False

    def writeErrors(self, failures, interfaces, jobId, shortColnames, writer, warningWriter, rowNumber):
        """ Write errors to error database

        Args:
            failures: List of errors to be written
            interfaces: InterfaceHolder object
            jobId: ID of current job
            shortColnames: Dict mapping short names to long names
            writer: CsvWriter object
            rowNumber: Current row number
        Returns:
            True if any fatal errors were found, False if only warnings are present
        """
        fatalErrorFound = False
        errorInterface = interfaces.errorDb
        # For each failure, record it in error report and metadata
        for failure in failures:
            # map short column names back to long names
            if failure[0] in shortColnames:
                fieldName = shortColnames[failure[0]]
            else:
                fieldName = failure[0]
            error = failure[1]
            failedValue = failure[2]
            originalRuleLabel = failure[3]

            severityId = interfaces.validationDb.getRuleSeverityId(failure[4])
            try:
                # If error is an int, it's one of our prestored messages
                errorType = int(error)
                errorMsg = ValidationError.getErrorMessage(errorType)
            except ValueError:
                # If not, treat it literally
                errorMsg = error
            if failure[4] == "fatal":
                fatalErrorFound = True
                writer.write([fieldName,errorMsg,str(rowNumber),failedValue,originalRuleLabel])
            elif failure[4] == "warning":
                # write to warnings file
                warningWriter.write([fieldName,errorMsg,str(rowNumber),failedValue,originalRuleLabel])
            errorInterface.recordRowError(jobId,self.filename,fieldName,error,rowNumber,originalRuleLabel,severity_id=severityId)
        return fatalErrorFound

    def runValidation(self, jobId, interfaces):
        """ Run validations for specified job
        Args:
            jobId: Job to be validated
            jobTracker: Interface for job tracker
        Returns:
            True if successful
        """

        CloudLogger.logError("VALIDATOR_INFO: ", "Beginning runValidation on jobID: "+str(jobId), "")

        jobTracker = interfaces.jobDb
        isFirstQuarter = jobTracker.checkFirstQuarter(jobId)
        submissionId = jobTracker.getSubmissionId(jobId)

        rowNumber = 1
        fileType = jobTracker.getFileType(jobId)
        # Clear existing records for this submission
        interfaces.stagingDb.clearFileBySubmission(submissionId,fileType)

        # Get short to long colname dictionary
        shortColnames = interfaces.validationDb.getShortToLongColname()

        # If local, make the error report directory
        if self.isLocal and not os.path.exists(self.directory):
            os.makedirs(self.directory)
        # Get bucket name and file name
        fileName = jobTracker.getFileName(jobId)
        self.filename = fileName
        bucketName = CONFIG_BROKER['aws_bucket']
        regionName = CONFIG_BROKER['aws_region']

        errorFileName = self.getFileName(jobTracker.getReportPath(jobId))
        warningFileName = self.getFileName(jobTracker.getWarningReportPath(jobId))

        # Create File Status object
        interfaces.errorDb.createFileIfNeeded(jobId,fileName)

        validationDB = interfaces.validationDb
        fieldList = validationDB.getFieldsByFileList(fileType)
        csvSchema = validationDB.getFieldsByFile(fileType, shortCols=True)

        reader = self.getReader()

        # Get file size and write to jobs table
        if CONFIG_BROKER["use_aws"]:
            fileSize = s3UrlHandler.getFileSize("errors/"+jobTracker.getReportPath(jobId))
        else:
            fileSize = os.path.getsize(jobTracker.getFileName(jobId))
        jobTracker.setFileSizeById(jobId, fileSize)

        fields = interfaces.validationDb.getFileColumnsByFile(fileType)
        try:
            # Pull file and return info on whether it's using short or long col headers
            reader.openFile(regionName, bucketName, fileName, fieldList,
                            bucketName, errorFileName)

            errorInterface = interfaces.errorDb
            self.longToShortDict = interfaces.validationDb.getLongToShortColname()
            # rowErrorPresent becomes true if any row error occurs, used for determining file status
            rowErrorPresent = False
            # list to keep track of rows that fail validations
            errorRows = []

            # While not done, pull one row and put it into staging table if it passes
            # the Validator

            with self.getWriter(regionName, bucketName, errorFileName, self.reportHeaders) as writer, \
                 self.getWriter(regionName, bucketName, warningFileName, self.reportHeaders) as warningWriter:
                while not reader.isFinished:
                    rowNumber += 1

                    if (rowNumber % 100) == 0:
                        CloudLogger.logError("VALIDATOR_INFO: ","JobId: "+str(jobId)+" loading row " + str(rowNumber),"")

                    #
                    # first phase of validations: read record and record a
                    # formatting error if there's a problem
                    #
                    (record, reduceRow, skipRow, doneReading, rowErrorHere) = self.readRecord(reader,writer,fileType,interfaces,rowNumber,jobId,isFirstQuarter, fields)
                    if reduceRow:
                        rowNumber -= 1
                    if rowErrorHere:
                        rowErrorPresent = True
                        errorRows.append(rowNumber)
                    if doneReading:
                        # Stop reading from input file
                        break
                    elif skipRow:
                        # Do not write this row to staging, but continue processing future rows
                        continue

                    #
                    # second phase of validations: do basic schema checks
                    # (e.g., require fields, field length, data type)
                    #
                    passedValidations, failures, valid = Validator.validate(record,csvSchema,fileType,interfaces)
                    if valid:
                        skipRow = self.writeToStaging(record, jobId, submissionId, passedValidations, interfaces, writer, rowNumber, fileType)
                        if skipRow:
                            rowErrorPresent = True
                            errorRows.append(rowNumber)
                            continue

                    if not passedValidations:
                        if self.writeErrors(failures, interfaces, jobId, shortColnames, writer, warningWriter, rowNumber):
                            rowErrorPresent = True
                            errorRows.append(rowNumber)

                interfaces.errorDb.setRowErrorsPresent(jobId,rowErrorPresent)
                CloudLogger.logError("VALIDATOR_INFO: ", "Loading complete on jobID: " + str(jobId) + ". Total rows added to staging: " + str(rowNumber), "")

                #
                # third phase of validations: run validation rules as specified
                # in the schema guidance. these validations are sql-based.
                #
                sqlErrorRows = self.runSqlValidations(
                    interfaces, jobId, fileType, shortColnames, writer, warningWriter, rowNumber)
                errorRows.extend(sqlErrorRows)

                # Write unfinished batch
                writer.finishBatch()
                warningWriter.finishBatch()

            # Calculate total number of rows in file
            # that passed validations
            errorRowsUnique = set(errorRows)
            totalRowsExcludingHeader = rowNumber - 1
            validRows = totalRowsExcludingHeader - len(errorRowsUnique)

            # Update job metadata
            jobTracker.setJobRowcounts(jobId, rowNumber, validRows)

            # Mark validation as finished in job tracker
            jobTracker.markJobStatus(jobId,"finished")
            errorInterface.writeAllRowErrors(jobId)
            # Update error info for submission
            jobTracker.populateSubmissionErrorInfo(submissionId)
        finally:
            # Ensure the file always closes
            reader.close()
            CloudLogger.logError("VALIDATOR_INFO: ", "Completed L1 and SQL rule validations on jobID: " + str(jobId), "")
        return True

    def runSqlValidations(self, interfaces, jobId, fileType, shortColnames, writer, warningWriter, rowNumber):
        """ Run all SQL rules for this file type

        Args:
            interfaces: InterfaceHolder object
            jobId: ID of current job
            fileType: Type of file for current job
            shortColnames: Dict mapping short field names to long
            writer: CsvWriter object
            waringWriter: CsvWriter for warnings
            rowNumber: Current row number

        Returns:
            a list of the row numbers that failed one of the sql-based validations
        """
        errorInterface = interfaces.errorDb
        errorRows = []
        sqlFailures = Validator.validateFileBySql(interfaces.jobDb.getSubmissionId(jobId),fileType,interfaces)
        for failure in sqlFailures:
            # convert shorter, machine friendly column names used in the
            # SQL validation queries back to their long names
            if failure[0] in shortColnames:
                fieldName = shortColnames[failure[0]]
            else:
                fieldName = failure[0]
            error = failure[1]
            failedValue = failure[2]
            row = failure[3]
            original_label = failure[4]
            fileTypeId = failure[5]
            targetFileId = failure[6]
            severityId = failure[7]
            if severityId == interfaces.validationDb.getRuleSeverityId("fatal"):
                errorRows.append(row)
            try:
                # If error is an int, it's one of our prestored messages
                errorType = int(error)
                errorMsg = ValidationError.getErrorMessage(errorType)
            except ValueError:
                # If not, treat it literally
                errorMsg = error
            if severityId == interfaces.validationDb.getRuleSeverityId("fatal"):
                writer.write([fieldName,errorMsg,str(row),failedValue,original_label])
            elif severityId == interfaces.validationDb.getRuleSeverityId("warning"):
                # write to warnings file
                warningWriter.write([fieldName,errorMsg,str(row),failedValue,original_label])
            errorInterface.recordRowError(jobId,self.filename,fieldName,
                                          error,rowNumber,original_label, file_type_id=fileTypeId, target_file_id = targetFileId, severity_id=severityId)
        return errorRows

    def runCrossValidation(self, jobId, interfaces):
        """ Cross file validation job, test all rules with matching rule_timing """
        validationDb = interfaces.validationDb
        errorDb = interfaces.errorDb
        submissionId = interfaces.jobDb.getSubmissionId(jobId)
        bucketName = CONFIG_BROKER['aws_bucket']
        regionName = CONFIG_BROKER['aws_region']
        CloudLogger.logError("VALIDATOR_INFO: ", "Beginning runCrossValidation on submissionID: "+str(submissionId), "")

        # Delete existing cross file errors for this submission
        errorDb.resetErrorsByJobId(jobId)

        # use db to get a list of the cross-file combinations
        targetFiles = validationDb.session.query(FileTypeValidation).subquery()
        crossFileCombos = validationDb.session.query(
            FileTypeValidation.name.label('first_file_name'),
            FileTypeValidation.file_id.label('first_file_id'),
            targetFiles.c.name.label('second_file_name'),
            targetFiles.c.file_id.label('second_file_id')
        ).filter(FileTypeValidation.file_order < targetFiles.c.file_order)

        # get all cross file rules from db
        crossFileRules = validationDb.session.query(RuleSql).filter(RuleSql.rule_cross_file_flag==True)

        # for each cross-file combo, run associated rules and create error report
        for row in crossFileCombos:
            comboRules = crossFileRules.filter(or_(and_(
                RuleSql.file_id==row.first_file_id,
                RuleSql.target_file_id==row.second_file_id), and_(
                RuleSql.file_id==row.second_file_id,
                RuleSql.target_file_id==row.first_file_id)))
            # send comboRules to validator.crossValidate sql
            failures = Validator.crossValidateSql(comboRules.all(),submissionId)
            # get error file name
            reportFilename = self.getFileName(interfaces.errorDb.getCrossReportName(submissionId, row.first_file_name, row.second_file_name))
            warningReportFilename = self.getFileName(interfaces.errorDb.getCrossWarningReportName(submissionId, row.first_file_name, row.second_file_name))

            # loop through failures to create the error report
            with self.getWriter(regionName, bucketName, reportFilename, self.crossFileReportHeaders) as writer, \
                 self.getWriter(regionName, bucketName, warningReportFilename, self.crossFileReportHeaders) as warningWriter:
                for failure in failures:
                    if failure[9] == interfaces.validationDb.getRuleSeverityId("fatal"):
                        writer.write(failure[0:7])
                    if failure[9] == interfaces.validationDb.getRuleSeverityId("warning"):
                        warningWriter.write(failure[0:7])
                    errorDb.recordRowError(jobId, "cross_file",
                        failure[0], failure[3], failure[5], failure[6], failure[7], failure[8], severity_id=failure[9])
                writer.finishBatch()
                warningWriter.finishBatch()

        errorDb.writeAllRowErrors(jobId)
        interfaces.jobDb.markJobStatus(jobId, "finished")
        CloudLogger.logError("VALIDATOR_INFO: ", "Completed runCrossValidation on submissionID: "+str(submissionId), "")
        # Update error info for submission
        interfaces.jobDb.populateSubmissionErrorInfo(submissionId)
        # TODO: Remove temporary step below
        # Temporarily set publishable flag at end of cross file, remove this once users are able to mark their submissions
        # as publishable
        # Publish only if no errors are present
        if interfaces.jobDb.getSubmissionById(submissionId).number_of_errors == 0:
            interfaces.jobDb.setPublishableFlag(submissionId, True)

    def validateJob(self, request,interfaces):
        """ Gets file for job, validates each row, and sends valid rows to a staging table
        Args:
        request -- HTTP request containing the jobId
        interfaces -- InterfaceHolder object to the databases
        Returns:
        Http response object
        """
        # Create connection to job tracker database
        self.filename = None
        jobId = None
        jobTracker = None

        try:
            jobTracker = interfaces.jobDb
            requestDict = RequestDictionary(request)
            if requestDict.exists("job_id"):
                jobId = requestDict.getValue("job_id")
            else:
                # Request does not have a job ID, can't validate
                raise ResponseException("No job ID specified in request",
                                        StatusCode.CLIENT_ERROR)

            # Check that job exists and is ready
            if not jobTracker.runChecks(jobId):
                raise ResponseException("Checks failed on Job ID",
                                        StatusCode.CLIENT_ERROR)
            jobType = interfaces.jobDb.checkJobType(jobId)

        except ResponseException as e:
            CloudLogger.logError(str(e), e, traceback.extract_tb(sys.exc_info()[2]))
            if e.errorType == None:
                # Error occurred while trying to get and check job ID
                e.errorType = ValidationError.jobError
            interfaces.errorDb.writeFileError(jobId, self.filename, e.errorType, e.extraInfo)
            return JsonResponse.error(e, e.status)
        except Exception as e:
            exc = ResponseException(str(e), StatusCode.INTERNAL_ERROR,type(e))
            CloudLogger.logError(str(e), e, traceback.extract_tb(sys.exc_info()[2]))
            self.markJob(jobId, jobTracker, "failed", interfaces.errorDb,
                self.filename, ValidationError.unknownError)
            return JsonResponse.error(exc, exc.status)

        try:
            jobTracker.markJobStatus(jobId, "running")
            if jobType == interfaces.jobDb.getJobTypeId("csv_record_validation"):
                self.runValidation(jobId, interfaces)
            elif jobType == interfaces.jobDb.getJobTypeId("validation"):
                self.runCrossValidation(jobId, interfaces)
            else:
                raise ResponseException("Bad job type for validator",
                    StatusCode.INTERNAL_ERROR)
            interfaces.errorDb.markFileComplete(jobId, self.filename)
            return JsonResponse.create(StatusCode.OK, {"message":"Validation complete"})
        except ResponseException as e:
            CloudLogger.logError(str(e), e, traceback.extract_tb(sys.exc_info()[2]))
            self.markJob(jobId, jobTracker, "invalid", interfaces.errorDb,
                self.filename,e.errorType, e.extraInfo)
            return JsonResponse.error(e, e.status)
        except ValueError as e:
            CloudLogger.logError(str(e), e, traceback.extract_tb(sys.exc_info()[2]))
            # Problem with CSV headers
            exc = ResponseException(str(e),StatusCode.CLIENT_ERROR,type(e), ValidationError.unknownError) #"Internal value error"
            self.markJob(jobId,jobTracker, "invalid", interfaces.errorDb, self.filename, ValidationError.unknownError)
            return JsonResponse.error(exc, exc.status)
        except Error as e:
            CloudLogger.logError(str(e),e,traceback.extract_tb(sys.exc_info()[2]))
            # CSV file not properly formatted (usually too much in one field)
            exc = ResponseException("Internal error",StatusCode.CLIENT_ERROR,type(e),ValidationError.unknownError)
            self.markJob(jobId,jobTracker,"invalid",interfaces.errorDb,self.filename,ValidationError.unknownError)
            return JsonResponse.error(exc, exc.status)
        except Exception as e:
            CloudLogger.logError(str(e), e, traceback.extract_tb(sys.exc_info()[2]))
            exc = ResponseException(str(e), StatusCode.INTERNAL_ERROR, type(e),
                ValidationError.unknownError)
            self.markJob(jobId, jobTracker, "failed", interfaces.errorDb,
                self.filename, ValidationError.unknownError)
            return JsonResponse.error(exc, exc.status)
