import os
import traceback
import sys
import copy
from csv import Error
from sqlalchemy import or_, and_
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.validationModels import FileType
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
            return "".join([self.directory, path])
        return "".join(["errors/", path])

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

        # Create File Status object
        interfaces.errorDb.createFileIfNeeded(jobId,fileName)

        validationDB = interfaces.validationDb
        fieldList = validationDB.getFieldsByFileList(fileType)
        csvSchema = validationDB.getFieldsByFile(fileType)
        rules = validationDB.getRulesByFile(fileType)

        reader = self.getReader()

        # Get file size and write to jobs table
        if CONFIG_BROKER["use_aws"]:
            fileSize = s3UrlHandler.getFileSize("errors/"+jobTracker.getReportPath(jobId))
        else:
            fileSize = os.path.getsize(jobTracker.getFileName(jobId))
        jobTracker.setFileSizeById(jobId, fileSize)


        try:
            # Pull file
            reader.openFile(regionName, bucketName, fileName, fieldList,
                            bucketName, errorFileName)

            errorInterface = interfaces.errorDb
            stagingInterface = interfaces.stagingDb

            # While not done, pull one row and put it into staging table if it passes
            # the Validator
            with self.getWriter(regionName, bucketName, errorFileName,
                                self.reportHeaders) as writer:
                while not reader.isFinished:
                    rowNumber += 1
                    if (rowNumber % 100) == 0:
                        CloudLogger.logError("VALIDATOR_INFO: ","JobId: "+str(jobId)+" loading row " + str(rowNumber),"")

                    try :
                        record = FieldCleaner.cleanRow(reader.getNextRecord(), fileType, interfaces.validationDb)
                        record["row_number"] = rowNumber
                        if reader.isFinished and len(record) < 2:
                            # This is the last line and is empty, don't record an error
                            rowNumber -= 1  # Don't count this row
                            break
                    except ResponseException as e:
                        if reader.isFinished and reader.extraLine:
                            #Last line may be blank don't record an error, reader.extraLine indicates a case where the last valid line has extra line breaks
                            # Don't count last row if empty
                            rowNumber -= 1
                        else:
                            writer.write(["Formatting Error", ValidationError.readErrorMsg, str(rowNumber), ""])
                            errorInterface.recordRowError(jobId,self.filename,"Formatting Error",ValidationError.readError,rowNumber)
                            errorInterface.setRowErrorsPresent(jobId, True)
                        continue
                    passedValidations, failures, valid  = Validator.validate(record,rules,csvSchema,fileType,interfaces)
                    if valid:
                        try:
                            record["job_id"] = jobId
                            record["submission_id"] = submissionId
                            record["valid_record"] = passedValidations
                            # temporary fix b/c we can't use '+4' as a column alias :(
                            if "primaryplaceofperformancezip+4" in record:
                                record["primaryplaceofperformancezipplus4"] = record["primaryplaceofperformancezip+4"]
                            stagingInterface.insertSubmissionRecordByFileType(record, fileType)
                        except ResponseException as e:
                            # Write failed, move to next record
                            writer.write(["Formatting Error", ValidationError.writeErrorMsg, str(rowNumber),""])
                            errorInterface.recordRowError(jobId, self.filename,
                                "Formatting Error",ValidationError.writeError, rowNumber)
                            errorInterface.setRowErrorsPresent(jobId, True)
                            continue

                    if not passedValidations:
                        # For each failure, record it in error report and metadata
                        if failures:
                            errorInterface.setRowErrorsPresent(jobId, True)
                        for failure in failures:
                            # map short column names back to long names
                            if failure[0] in shortColnames:
                                fieldName = shortColnames[failure[0]]
                            else:
                                fieldName = failure[0]
                            error = failure[1]
                            failedValue = failure[2]
                            originalRuleLabel = failure[3]
                            try:
                                # If error is an int, it's one of our prestored messages
                                errorType = int(error)
                                errorMsg = ValidationError.getErrorMessage(errorType)
                            except ValueError:
                                # If not, treat it literally
                                errorMsg = error
                            writer.write([fieldName,errorMsg,str(rowNumber),failedValue,originalRuleLabel])
                            errorInterface.recordRowError(jobId,self.filename,fieldName,error,rowNumber,originalRuleLabel)
                CloudLogger.logError("VALIDATOR_INFO: ", "Loading complete on jobID: " + str(jobId) + ". Total rows added to staging: " + str(rowNumber), "")
                # Do SQL validations for this file
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
                    try:
                        # If error is an int, it's one of our prestored messages
                        errorType = int(error)
                        errorMsg = ValidationError.getErrorMessage(errorType)
                    except ValueError:
                        # If not, treat it literally
                        errorMsg = error
                    writer.write([fieldName,errorMsg,str(row),failedValue,original_label])
                    errorInterface.recordRowError(jobId,self.filename,fieldName,
                                                  error,rowNumber,original_label, file_type_id=fileTypeId, target_file_id = targetFileId)

                # Write unfinished batch
                writer.finishBatch()

            # Write number of rows to job table
            jobTracker.setNumberOfRowsById(jobId,rowNumber)
            # Mark validation as finished in job tracker
            jobTracker.markJobStatus(jobId,"finished")
            errorInterface.writeAllRowErrors(jobId)
        finally:
            # Ensure the file always closes
            reader.close()
            CloudLogger.logError("VALIDATOR_INFO: ", "Completed L1 and SQL rule validations on jobID: " + str(jobId), "")
        return True

    def runCrossValidation(self, jobId, interfaces):
        """ Cross file validation job, test all rules with matching rule_timing """
        validationDb = interfaces.validationDb
        errorDb = interfaces.errorDb
        submissionId = interfaces.jobDb.getSubmissionId(jobId)
        bucketName = CONFIG_BROKER['aws_bucket']
        regionName = CONFIG_BROKER['aws_region']
        CloudLogger.logError("VALIDATOR_INFO: ", "Beginning runCrossValidation on submissionID: "+str(submissionId), "")


        # use db to get a list of the cross-file combinations
        targetFiles = validationDb.session.query(FileType).subquery()
        crossFileCombos = validationDb.session.query(
            FileType.name.label('first_file_name'),
            FileType.file_id.label('first_file_id'),
            targetFiles.c.name.label('second_file_name'),
            targetFiles.c.file_id.label('second_file_id')
        ).filter(FileType.file_order < targetFiles.c.file_order)

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

            # loop through failures to create the error report
            with self.getWriter(regionName, bucketName, reportFilename,
                                    self.crossFileReportHeaders) as writer:
                for failure in failures:
                    writer.write(failure[0:7])
                    errorDb.recordRowError(jobId, "cross_file",
                        failure[0], failure[3], failure[5], failure[6], failure[7], failure[8])
                writer.finishBatch()

        errorDb.writeAllRowErrors(jobId)
        interfaces.jobDb.markJobStatus(jobId, "finished")
        CloudLogger.logError("VALIDATOR_INFO: ", "Completed runCrossValidation on submissionID: "+str(submissionId), "")

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
