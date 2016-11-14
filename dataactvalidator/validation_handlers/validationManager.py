from csv import Error
import os
import logging

from sqlalchemy import or_, and_
from sqlalchemy.orm import joinedload

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.lookups import FILE_TYPE, FILE_TYPE_DICT, RULE_SEVERITY_DICT
from dataactcore.models.validationModels import FileColumn
from dataactcore.interfaces.function_bag import (
    createFileIfNeeded, writeFileError, markFileComplete, run_job_checks)
from dataactcore.models.errorModels import ErrorMetadata
from dataactcore.models.jobModels import Job
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.report import (
    get_report_path, get_cross_warning_report_name, get_cross_report_name, get_cross_file_pairs)
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactvalidator.filestreaming.csvS3Reader import CsvS3Reader
from dataactvalidator.filestreaming.csvLocalReader import CsvLocalReader
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer
from dataactvalidator.validation_handlers.errorInterface import ErrorInterface
from dataactvalidator.validation_handlers.validator import Validator
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactcore.models.validationModels import RuleSql


_exception_logger = logging.getLogger('deprecated.exception')


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

        # create long-to-short (and vice-versa) column name mappings
        sess = GlobalDB.db().session
        colnames = sess.query(FileColumn.name, FileColumn.name_short).all()
        self.long_to_short_dict = {row.name: row.name_short for row in colnames}
        self.short_to_long_dict = {row.name_short: row.name for row in colnames}

    @staticmethod
    def markJob(job_id,jobTracker,status,filename=None, fileError = ValidationError.unknownError, extraInfo = None):
        """ Update status of a job in job tracker database
        Args:
            job_id: Job to be updated
            jobTracker: Interface object for job tracker
            status: New status for specified job
            filename: Filename of file to be validated
            fileError: Type of error that occurred if this is an invalid or failed status
            extraInfo: Dict of extra fields to attach to exception
        """
        try:
            if filename != None and (status == "invalid" or status == "failed"):
                # Mark the file error that occurred
                writeFileError(job_id, filename, fileError, extraInfo)
            jobTracker.markJobStatus(job_id, status)
        except ResponseException as e:
            # Could not get a unique job ID in the database, either a bad job ID was passed in
            # or the record of that job was lost.
            # Either way, cannot mark status of a job that does not exist
            # Log error
            JsonResponse.error(e, e.status)

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

    def readRecord(self,reader,writer,file_type,interfaces,row_number,job_id,fields,error_list):
        """ Read and process the next record

        Args:
            reader: CsvReader object
            writer: CsvWriter object
            file_type: Type of file for current job
            interfaces: InterfaceHolder object
            row_number: Next row number to be read
            job_id: ID of current job
            fields: List of FileColumn objects for this file type
            error_list: instance of ErrorInterface to keep track of errors

        Returns:
            Tuple with four elements:
            1. Dict of record after preprocessing
            2. Boolean indicating whether to reduce row count
            3. Boolean indicating whether to skip row
            4. Boolean indicating whether to stop reading
            5. Row error has been found
        """
        reduce_row = False
        row_error_found = False
        try:

            record = FieldCleaner.cleanRow(reader.get_next_record(), self.long_to_short_dict, fields)
            record["row_number"] = row_number
            if reader.is_finished and len(record) < 2:
                # This is the last line and is empty, don't record an error
                return {}, True, True, True, False  # Don't count this row
        except ResponseException:
            if reader.is_finished and reader.extra_line:
                #Last line may be blank don't record an error, reader.extra_line indicates a case where the last valid line has extra line breaks
                # Don't count last row if empty
                reduce_row = True
            else:
                writer.write(["Formatting Error", ValidationError.readErrorMsg, str(row_number), ""])
                error_list.recordRowError(job_id, self.filename, "Formatting Error", ValidationError.readError,
                                          row_number, severity_id=RULE_SEVERITY_DICT['fatal'])
                row_error_found = True
            return {}, reduce_row, True, False, row_error_found
        return record, reduce_row, False, False, row_error_found

    def writeToStaging(self, record, job_id, submission_id, passed_validations, writer, row_number, model, error_list):
        """ Write this record to the staging tables

        Args:
            record: Record to be written
            job_id: ID of current job
            submission_id: ID of current submission
            passed_validations: True if record has not failed first validations
            writer: CsvWriter object
            row_number: Current row number
            model: orm model for the current file
            error_list: instance of ErrorInterface to keep track of errors

        Returns:
            Boolean indicating whether to skip current row
        """
        sess = GlobalDB.db().session
        try:
            record["job_id"] = job_id
            record["submission_id"] = submission_id
            record["valid_record"] = passed_validations
            sess.add(model(**record))
            sess.commit()

        except ResponseException:
            # Write failed, move to next record
            writer.write(["Formatting Error", ValidationError.writeErrorMsg, row_number,""])
            error_list.recordRowError(job_id, self.filename,
                "Formatting Error", ValidationError.writeError, row_number, severity_id=RULE_SEVERITY_DICT['fatal'])
            return True
        return False

    def writeErrors(self, failures, interfaces, job_id, short_colnames, writer, warning_writer, row_number, error_list):
        """ Write errors to error database

        Args:
            failures: List of errors to be written
            interfaces: InterfaceHolder object
            job_id: ID of current job
            short_colnames: Dict mapping short names to long names
            writer: CsvWriter object
            warning_writer: CsvWriter object
            row_number: Current row number
            error_list: instance of ErrorInterface to keep track of errors
        Returns:
            True if any fatal errors were found, False if only warnings are present
        """
        fatal_error_found = False
        # For each failure, record it in error report and metadata
        for failure in failures:
            # map short column names back to long names
            if failure[0] in short_colnames:
                field_name = short_colnames[failure[0]]
            else:
                field_name = failure[0]
            error = failure[1]
            failed_value = failure[2]
            original_rule_label = failure[3]

            severityId = RULE_SEVERITY_DICT[failure[4]]
            try:
                # If error is an int, it's one of our prestored messages
                error_type = int(error)
                error_msg = ValidationError.getErrorMessage(error_type)
            except ValueError:
                # If not, treat it literally
                error_msg = error
            if failure[4] == "fatal":
                fatal_error_found = True
                writer.write([field_name,error_msg,str(row_number),failed_value,original_rule_label])
            elif failure[4] == "warning":
                # write to warnings file
                warning_writer.write([field_name,error_msg,str(row_number),failed_value,original_rule_label])
            error_list.recordRowError(job_id,self.filename,field_name,error,row_number,original_rule_label,severity_id=severityId)
        return fatal_error_found

    def runValidation(self, job_id, interfaces):
        """ Run validations for specified job
        Args:
            job_id: Job to be validated
            interfaces: All interfaces
        Returns:
            True if successful
        """

        sess = GlobalDB.db().session
        # get the job object here so we can call the refactored getReportPath
        # todo: replace other db access functions with job object attributes
        job = sess.query(Job).filter(Job.job_id == job_id).one()

        error_list = ErrorInterface()

        _exception_logger.info(
            'VALIDATOR_INFO: Beginning runValidation on job_id: %s', job_id)

        jobTracker = interfaces.jobDb
        submissionId = jobTracker.getSubmissionId(job_id)

        rowNumber = 1
        fileType = jobTracker.getFileType(job_id)
        # Get orm model for this file
        model = [ft.model for ft in FILE_TYPE if ft.name == fileType][0]

        # Clear existing records for this submission
        sess.query(model).filter(model.submission_id == submissionId).delete()
        sess.commit()

        # If local, make the error report directory
        if self.isLocal and not os.path.exists(self.directory):
            os.makedirs(self.directory)
        # Get bucket name and file name
        fileName = jobTracker.getFileName(job_id)
        self.filename = fileName
        bucketName = CONFIG_BROKER['aws_bucket']
        regionName = CONFIG_BROKER['aws_region']

        errorFileName = self.getFileName(get_report_path(job, 'error'))
        warningFileName = self.getFileName(get_report_path(job, 'warning'))

        # Create File Status object
        createFileIfNeeded(job_id,fileName)

        reader = self.getReader()

        # Get file size and write to jobs table
        if CONFIG_BROKER["use_aws"]:
            fileSize = s3UrlHandler.getFileSize(errorFileName)
        else:
            fileSize = os.path.getsize(jobTracker.getFileName(job_id))
        jobTracker.setFileSizeById(job_id, fileSize)

        # Get fields for this file
        fields = sess.query(FileColumn). \
            options(joinedload('field_type')). \
            filter(FileColumn.file_id == FILE_TYPE_DICT[fileType]). \
            all()
        csvSchema = {row.name_short: row for row in fields}

        try:
            # Pull file and return info on whether it's using short or long col headers
            reader.open_file(regionName, bucketName, fileName, fields,
                             bucketName, errorFileName, self.long_to_short_dict)

            # list to keep track of rows that fail validations
            errorRows = []

            # While not done, pull one row and put it into staging table if it passes
            # the Validator

            with self.getWriter(regionName, bucketName, errorFileName, self.reportHeaders) as writer, \
                 self.getWriter(regionName, bucketName, warningFileName, self.reportHeaders) as warningWriter:
                while not reader.is_finished:
                    rowNumber += 1

                    if (rowNumber % 100) == 0:
                        _exception_logger.info(
                            'VALIDATOR_INFO: JobId: %s loading row %s',
                            job_id, rowNumber)

                    #
                    # first phase of validations: read record and record a
                    # formatting error if there's a problem
                    #
                    (record, reduceRow, skipRow, doneReading, rowErrorHere) = self.readRecord(reader,writer,fileType,interfaces,rowNumber,job_id,fields,error_list)
                    if reduceRow:
                        rowNumber -= 1
                    if rowErrorHere:
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
                    # D files are obtained from upstream systems (ASP and FPDS) that perform their own basic validations,
                    # so these validations are not repeated here
                    if fileType in ["award", "award_procurement"]:
                        # Skip basic validations for D files, set as valid to trigger write to staging
                        passedValidations = True
                        valid = True
                    else:
                        passedValidations, failures, valid = Validator.validate(record, csvSchema)
                    if valid:
                        skipRow = self.writeToStaging(record, job_id, submissionId, passedValidations, writer, rowNumber, model, error_list)
                        if skipRow:
                            errorRows.append(rowNumber)
                            continue

                    if not passedValidations:
                        if self.writeErrors(failures, interfaces, job_id, self.short_to_long_dict, writer, warningWriter, rowNumber, error_list):
                            errorRows.append(rowNumber)

                _exception_logger.info(
                    'VALIDATOR_INFO: Loading complete on job_id: %s. '
                    'Total rows added to staging: %s', job_id, rowNumber)

                #
                # third phase of validations: run validation rules as specified
                # in the schema guidance. these validations are sql-based.
                #
                sqlErrorRows = self.runSqlValidations(
                    interfaces, job_id, fileType, self.short_to_long_dict, writer, warningWriter, rowNumber, error_list)
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
            jobTracker.setJobRowcounts(job_id, rowNumber, validRows)

            error_list.writeAllRowErrors(job_id)
            # Update error info for submission
            jobTracker.populateSubmissionErrorInfo(submissionId)
            # Mark validation as finished in job tracker
            jobTracker.markJobStatus(job_id,"finished")
            markFileComplete(job_id, self.filename)
        finally:
            # Ensure the file always closes
            reader.close()
            _exception_logger.info(
                'VALIDATOR_INFO: Completed L1 and SQL rule validations on '
                'job_id: %s', job_id)
        return True

    def runSqlValidations(self, interfaces, job_id, file_type, short_colnames, writer, warning_writer, row_number, error_list):
        """ Run all SQL rules for this file type

        Args:
            interfaces: InterfaceHolder object
            job_id: ID of current job
            file_type: Type of file for current job
            short_colnames: Dict mapping short field names to long
            writer: CsvWriter object
            warning_writer: CsvWriter for warnings
            row_number: Current row number
            error_list: instance of ErrorInterface to keep track of errors

        Returns:
            a list of the row numbers that failed one of the sql-based validations
        """
        error_rows = []
        sql_failures = Validator.validateFileBySql(
            interfaces.jobDb.getSubmissionId(job_id), file_type, self.short_to_long_dict)
        for failure in sql_failures:
            # convert shorter, machine friendly column names used in the
            # SQL validation queries back to their long names
            if failure[0] in short_colnames:
                field_name = short_colnames[failure[0]]
            else:
                field_name = failure[0]
            error = failure[1]
            failed_value = failure[2]
            row = failure[3]
            original_label = failure[4]
            file_type_id = failure[5]
            target_file_id = failure[6]
            severity_id = failure[7]
            if severity_id == RULE_SEVERITY_DICT['fatal']:
                error_rows.append(row)
            try:
                # If error is an int, it's one of our prestored messages
                error_type = int(error)
                error_msg = ValidationError.getErrorMessage(error_type)
            except ValueError:
                # If not, treat it literally
                error_msg = error
            if severity_id == RULE_SEVERITY_DICT['fatal']:
                writer.write([field_name,error_msg,str(row),failed_value,original_label])
            elif severity_id == RULE_SEVERITY_DICT['warning']:
                # write to warnings file
                warning_writer.write([field_name,error_msg,str(row),failed_value,original_label])
            error_list.recordRowError(job_id,self.filename,field_name,
                                          error,row_number,original_label, file_type_id=file_type_id, target_file_id = target_file_id, severity_id=severity_id)
        return error_rows

    def runCrossValidation(self, job_id, interfaces):
        """ Cross file validation job, test all rules with matching rule_timing """
        sess = GlobalDB.db().session
        # Create File Status object
        createFileIfNeeded(job_id)
        error_list = ErrorInterface()
        
        submission_id = interfaces.jobDb.getSubmissionId(job_id)
        bucketName = CONFIG_BROKER['aws_bucket']
        regionName = CONFIG_BROKER['aws_region']
        _exception_logger.info(
            'VALIDATOR_INFO: Beginning runCrossValidation on submission_id: '
            '%s', submission_id)

        # Delete existing cross file errors for this submission
        sess.query(ErrorMetadata).filter(ErrorMetadata.job_id == job_id).delete()
        sess.commit()

        # get all cross file rules from db
        crossFileRules = sess.query(RuleSql).filter(RuleSql.rule_cross_file_flag==True)

        # for each cross-file combo, run associated rules and create error report
        for c in get_cross_file_pairs():
            first_file = c[0]
            second_file = c[1]
            comboRules = crossFileRules.filter(or_(and_(
                RuleSql.file_id==first_file.id,
                RuleSql.target_file_id==second_file.id), and_(
                RuleSql.file_id==second_file.id,
                RuleSql.target_file_id==first_file.id)))
            # send comboRules to validator.crossValidate sql
            failures = Validator.crossValidateSql(comboRules.all(), submission_id, self.short_to_long_dict)
            # get error file name
            reportFilename = self.getFileName(get_cross_report_name(submission_id, first_file.name, second_file.name))
            warningReportFilename = self.getFileName(get_cross_warning_report_name(submission_id, first_file.name, second_file.name))

            # loop through failures to create the error report
            with self.getWriter(regionName, bucketName, reportFilename, self.crossFileReportHeaders) as writer, \
                 self.getWriter(regionName, bucketName, warningReportFilename, self.crossFileReportHeaders) as warningWriter:
                for failure in failures:
                    if failure[9] == RULE_SEVERITY_DICT['fatal']:
                        writer.write(failure[0:7])
                    if failure[9] == RULE_SEVERITY_DICT['warning']:
                        warningWriter.write(failure[0:7])
                    error_list.recordRowError(job_id, "cross_file",
                        failure[0], failure[3], failure[5], failure[6], failure[7], failure[8], severity_id=failure[9])
                writer.finishBatch()
                warningWriter.finishBatch()

        error_list.writeAllRowErrors(job_id)
        interfaces.jobDb.markJobStatus(job_id, "finished")
        _exception_logger.info(
            'VALIDATOR_INFO: Completed runCrossValidation on submission_id: '
            '%s', submission_id)
        # Update error info for submission
        interfaces.jobDb.populateSubmissionErrorInfo(submission_id)
        # TODO: Remove temporary step below
        # Temporarily set publishable flag at end of cross file, remove this once users are able to mark their submissions
        # as publishable
        # Publish only if no errors are present
        if interfaces.jobDb.getSubmissionById(submission_id).number_of_errors == 0:
            interfaces.jobDb.setPublishableFlag(submission_id, True)

        # Mark validation complete
        markFileComplete(job_id)

    def validate_job(self, request, interfaces):
        """ Gets file for job, validates each row, and sends valid rows to a staging table
        Args:
        request -- HTTP request containing the jobId
        interfaces -- InterfaceHolder object to the databases
        Returns:
        Http response object
        """
        # Create connection to job tracker database
        self.filename = None
        sess = GlobalDB.db().session

        jobTracker = interfaces.jobDb
        requestDict = RequestDictionary(request)
        if requestDict.exists('job_id'):
            job_id = requestDict.getValue('job_id')
        else:
            # Request does not have a job ID, can't validate
            validation_error_type = ValidationError.jobError
            raise ResponseException('No job ID specified in request',
                                    StatusCode.CLIENT_ERROR, None,
                                    validation_error_type)

        # Get the job
        job = sess.query(Job).filter_by(job_id=job_id).one_or_none()
        if job is None:
            validation_error_type = ValidationError.jobError
            writeFileError(job_id, self.filename, validation_error_type)
            raise ResponseException('Job ID {} not found in database'.format(job_id),
                                    StatusCode.CLIENT_ERROR, None,
                                    validation_error_type)

        # Make sure job's prerequisites are complete
        if not run_job_checks(job_id):
            validation_error_type = ValidationError.jobError
            writeFileError(job_id, self.filename, validation_error_type)
            raise ResponseException('Prerequisites for Job ID {} are not complete'.format(job_id),
                                    StatusCode.CLIENT_ERROR, None,
                                    validation_error_type)

        # Make sure this is a validation job
        if job.job_type.name in ('csv_record_validation', 'validation'):
            job_type_name = job.job_type.name
        else:
            validation_error_type = ValidationError.jobError
            writeFileError(job_id, self.filename, validation_error_type)
            raise ResponseException(
                'Job ID {} is not a validation job (job type is {})'.format(job_id, job.job_type.name),
                StatusCode.CLIENT_ERROR, None,
                validation_error_type)

        # todo: remove the following try/catch once 1st batch of changes are merged
        try:
            jobTracker.markJobStatus(job_id, "running")
            if job_type_name == 'csv_record_validation':
                self.runValidation(job_id, interfaces)
            elif job_type_name == 'validation':
                self.runCrossValidation(job_id, interfaces)
            else:
                raise ResponseException("Bad job type for validator",
                    StatusCode.INTERNAL_ERROR)

            return JsonResponse.create(StatusCode.OK, {"message":"Validation complete"})
        except ResponseException as e:
            _exception_logger.exception(str(e))
            self.markJob(job_id, jobTracker, "invalid", self.filename,e.errorType, e.extraInfo)
            return JsonResponse.error(e, e.status)
        except ValueError as e:
            _exception_logger.exception(str(e))
            # Problem with CSV headers
            exc = ResponseException(str(e),StatusCode.CLIENT_ERROR,type(e), ValidationError.unknownError) #"Internal value error"
            self.markJob(job_id,jobTracker, "invalid", self.filename, ValidationError.unknownError)
            return JsonResponse.error(exc, exc.status)
        except Error as e:
            _exception_logger.exception(str(e))
            # CSV file not properly formatted (usually too much in one field)
            exc = ResponseException("Internal error",StatusCode.CLIENT_ERROR,type(e),ValidationError.unknownError)
            self.markJob(job_id,jobTracker,"invalid",self.filename,ValidationError.unknownError)
            return JsonResponse.error(exc, exc.status)
        except Exception as e:
            _exception_logger.exception(str(e))
            exc = ResponseException(str(e), StatusCode.INTERNAL_ERROR, type(e),
                ValidationError.unknownError)
            self.markJob(job_id, jobTracker, "failed", self.filename, ValidationError.unknownError)
            return JsonResponse.error(exc, exc.status)
