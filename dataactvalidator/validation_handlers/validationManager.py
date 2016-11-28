import os
import logging

from sqlalchemy import and_, or_
from sqlalchemy.orm import joinedload

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.lookups import FILE_TYPE, FILE_TYPE_DICT, RULE_SEVERITY_DICT
from dataactcore.models.jobModels import Submission
from dataactcore.models.validationModels import FileColumn
from dataactcore.interfaces.function_bag import (
    createFileIfNeeded, writeFileError, markFileComplete, run_job_checks,
    mark_job_status, sumNumberOfErrorsForJobList, populateSubmissionErrorInfo
)
from dataactcore.models.errorModels import ErrorMetadata
from dataactcore.models.jobModels import Job
from dataactcore.models.stagingModels import FlexField
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
        self.isLocal = isLocal
        self.directory = directory

        # create long-to-short (and vice-versa) column name mappings
        sess = GlobalDB.db().session
        colnames = sess.query(FileColumn.name, FileColumn.name_short).all()
        self.long_to_short_dict = {row.name: row.name_short for row in colnames}
        self.short_to_long_dict = {row.name_short: row.name for row in colnames}

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

    def readRecord(self,reader,writer,file_type,row_number,job,fields,error_list):
        """ Read and process the next record

        Args:
            reader: CsvReader object
            writer: CsvWriter object
            file_type: Type of file for current job
            row_number: Next row number to be read
            job_id: ID of current job
            fields: List of FileColumn objects for this file type
            error_list: instance of ErrorInterface to keep track of errors

        Returns:
            Tuple with six elements:
            1. Dict of record after preprocessing
            2. Boolean indicating whether to reduce row count
            3. Boolean indicating whether to skip row
            4. Boolean indicating whether to stop reading
            5. Row error has been found
            6. Dict of flex columns
        """
        reduce_row = False
        row_error_found = False
        job_id = job.job_id
        try:
            (next_record, flex_cols) = reader.get_next_record()
            record = FieldCleaner.cleanRow(next_record, self.long_to_short_dict, fields)
            record["row_number"] = row_number
            if flex_cols:
                flex_cols["row_number"] = row_number

            if reader.is_finished and len(record) < 2:
                # This is the last line and is empty, don't record an error
                return {}, True, True, True, False, {}  # Don't count this row
        except ResponseException:
            if reader.is_finished and reader.extra_line:
                #Last line may be blank don't record an error, reader.extra_line indicates a case where the last valid line has extra line breaks
                # Don't count last row if empty
                reduce_row = True
            else:
                writer.write(["Formatting Error", ValidationError.readErrorMsg, str(row_number), ""])
                error_list.recordRowError(job_id, job.filename, "Formatting Error", ValidationError.readError,
                                          row_number, severity_id=RULE_SEVERITY_DICT['fatal'])
                row_error_found = True

            return {}, reduce_row, True, False, row_error_found, {}
        return record, reduce_row, False, False, row_error_found, flex_cols

    def writeToStaging(self, record, job, submission_id, passed_validations, writer, row_number, model, error_list):
        """ Write this record to the staging tables

        Args:
            record: Record to be written
            job: Current job
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
        job_id = job.job_id
        try:
            record["job_id"] = job_id
            record["submission_id"] = submission_id
            record["valid_record"] = passed_validations
            sess.add(model(**record))
            sess.commit()

        except ResponseException:
            # Write failed, move to next record
            writer.write(["Formatting Error", ValidationError.writeErrorMsg, row_number,""])
            error_list.recordRowError(job_id, job.filename,
                "Formatting Error", ValidationError.writeError, row_number, severity_id=RULE_SEVERITY_DICT['fatal'])
            return True
        return False

    def writeErrors(self, failures, job, short_colnames, writer, warning_writer, row_number, error_list):
        """ Write errors to error database

        Args:
            failures: List of errors to be written
            job: Current job
            short_colnames: Dict mapping short names to long names
            writer: CsvWriter object
            warning_writer: CsvWriter object
            row_number: Current row number
            error_list: instance of ErrorInterface to keep track of errors
        Returns:
            True if any fatal errors were found, False if only warnings are present
        """
        job_id = job.job_id
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
            error_list.recordRowError(job_id,job.filename,field_name,error,row_number,original_rule_label,severity_id=severityId)
        return fatal_error_found

    def write_to_flex(self, flex_cols, job_id, submission_id, file_type):
        """ Write this record to the staging tables

        Args:
            flex_cols: Record to be written
            job_id: ID of current job
            submission_id: ID of current submission
            file_type: Type of file for current job

        Returns:
            Boolean indicating whether to skip current row
        """
        sess = GlobalDB.db().session

        flex_cols["job_id"] = job_id
        flex_cols["submission_id"] = submission_id

        sess.add(FlexField(**flex_cols))
        sess.commit()

    def runValidation(self, job):
        """ Run validations for specified job
        Args:
            job: Job to be validated
        Returns:
            True if successful
        """

        sess = GlobalDB.db().session
        job_id = job.job_id

        error_list = ErrorInterface()

        _exception_logger.info(
            'VALIDATOR_INFO: Beginning runValidation on job_id: %s', job_id)

        submission_id = job.submission_id

        rowNumber = 1
        fileType = job.file_type.name
        # Get orm model for this file
        model = [ft.model for ft in FILE_TYPE if ft.name == fileType][0]

        # Clear existing records for this submission
        sess.query(model).filter_by(submission_id=submission_id).delete()
        sess.commit()

        # If local, make the error report directory
        if self.isLocal and not os.path.exists(self.directory):
            os.makedirs(self.directory)
        # Get bucket name and file name
        fileName = job.filename
        bucketName = CONFIG_BROKER['aws_bucket']
        regionName = CONFIG_BROKER['aws_region']

        errorFileName = self.getFileName(get_report_path(job, 'error'))
        warningFileName = self.getFileName(get_report_path(job, 'warning'))

        # Create File Status object
        createFileIfNeeded(job_id, fileName)

        reader = self.getReader()

        # Get file size and write to jobs table
        if CONFIG_BROKER["use_aws"]:
            fileSize = s3UrlHandler.getFileSize(errorFileName)
        else:
            fileSize = os.path.getsize(fileName)
        job.file_size = fileSize
        sess.commit()

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
                    (record, reduceRow, skipRow, doneReading, rowErrorHere, flex_cols) = self.readRecord(reader, writer, fileType, rowNumber, job, fields, error_list)
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
                        skipRow = self.writeToStaging(
                            record, job, submission_id, passedValidations,
                            writer, rowNumber, model, error_list)
                        if flex_cols:
                            self.write_to_flex(flex_cols, job_id, submission_id, fileType)

                        if skipRow:
                            errorRows.append(rowNumber)
                            continue

                    if not passedValidations:
                        if self.writeErrors(failures, job, self.short_to_long_dict, writer, warningWriter, rowNumber, error_list):
                            errorRows.append(rowNumber)

                _exception_logger.info(
                    'VALIDATOR_INFO: Loading complete on job_id: %s. '
                    'Total rows added to staging: %s', job_id, rowNumber)

                if fileType in ('appropriations', 'program_activity',
                                'award_financial'):
                    update_tas_ids(model, submission_id)
                #
                # third phase of validations: run validation rules as specified
                # in the schema guidance. these validations are sql-based.
                #
                sqlErrorRows = self.runSqlValidations(
                    job, fileType, self.short_to_long_dict, writer, warningWriter, rowNumber, error_list)
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
            job.number_of_rows = rowNumber
            job.number_of_rows_valid = validRows
            sess.commit()

            error_list.writeAllRowErrors(job_id)
            # Update error info for submission
            populateSubmissionErrorInfo(submission_id)
            # Mark validation as finished in job tracker
            mark_job_status(job_id, "finished")
            markFileComplete(job_id, fileName)
        finally:
            # Ensure the file always closes
            reader.close()
            _exception_logger.info(
                'VALIDATOR_INFO: Completed L1 and SQL rule validations on '
                'job_id: %s', job_id)
        return True

    def runSqlValidations(self, job, file_type, short_colnames, writer, warning_writer, row_number, error_list):
        """ Run all SQL rules for this file type

        Args:
            job: Current job
            file_type: Type of file for current job
            short_colnames: Dict mapping short field names to long
            writer: CsvWriter object
            warning_writer: CsvWriter for warnings
            row_number: Current row number
            error_list: instance of ErrorInterface to keep track of errors

        Returns:
            a list of the row numbers that failed one of the sql-based validations
        """
        sess = GlobalDB.db().session
        job_id = job.job_id
        error_rows = []
        sql_failures = Validator.validateFileBySql(
            job.submission_id, file_type, self.short_to_long_dict)
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
            error_list.recordRowError(job_id,job.filename,field_name,
                                          error,row_number,original_label, file_type_id=file_type_id, target_file_id = target_file_id, severity_id=severity_id)
        return error_rows

    def runCrossValidation(self, job):
        """ Cross file validation job, test all rules with matching rule_timing """
        sess = GlobalDB.db().session
        job_id = job.job_id
        # Create File Status object
        createFileIfNeeded(job_id)
        error_list = ErrorInterface()
        
        submission_id = job.submission_id
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
        mark_job_status(job_id, "finished")
        _exception_logger.info(
            'VALIDATOR_INFO: Completed runCrossValidation on submission_id: '
            '%s', submission_id)
        submission = sess.query(Submission).filter_by(submission_id = submission_id).one()
        # Update error info for submission
        submission.number_of_errors = sumNumberOfErrorsForJobList(submission_id)
        submission.number_of_warnings = sumNumberOfErrorsForJobList(submission_id, errorType="warning")
        # TODO: Remove temporary step below
        # Temporarily set publishable flag at end of cross file, remove this once users are able to mark their submissions
        # as publishable
        # Publish only if no errors are present
        if submission.number_of_errors == 0:
            submission.publishable = True
        sess.commit()

        # Mark validation complete
        markFileComplete(job_id)

    def validate_job(self, request):
        """ Gets file for job, validates each row, and sends valid rows to a staging table
        Args:
        request -- HTTP request containing the jobId
        Returns:
        Http response object
        """
        # Create connection to job tracker database
        sess = GlobalDB.db().session

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
            writeFileError(job_id, None, validation_error_type)
            raise ResponseException('Job ID {} not found in database'.format(job_id),
                                    StatusCode.CLIENT_ERROR, None,
                                    validation_error_type)

        # Make sure job's prerequisites are complete
        if not run_job_checks(job_id):
            validation_error_type = ValidationError.jobError
            writeFileError(job_id, None, validation_error_type)
            raise ResponseException('Prerequisites for Job ID {} are not complete'.format(job_id),
                                    StatusCode.CLIENT_ERROR, None,
                                    validation_error_type)

        # Make sure this is a validation job
        if job.job_type.name in ('csv_record_validation', 'validation'):
            job_type_name = job.job_type.name
        else:
            validation_error_type = ValidationError.jobError
            writeFileError(job_id, None, validation_error_type)
            raise ResponseException(
                'Job ID {} is not a validation job (job type is {})'.format(job_id, job.job_type.name),
                StatusCode.CLIENT_ERROR, None,
                validation_error_type)

        # set job status to running and do validations
        mark_job_status(job_id, "running")
        if job_type_name == 'csv_record_validation':
            self.runValidation(job)
        elif job_type_name == 'validation':
            self.runCrossValidation(job)
        else:
            raise ResponseException("Bad job type for validator",
                StatusCode.INTERNAL_ERROR)

        return JsonResponse.create(StatusCode.OK, {"message":"Validation complete"})

def update_tas_ids(model, submission_id):
    sess = GlobalDB.db().session
    submission = sess.query(Submission).\
        filter_by(submission_id=submission_id).one()

    # Due to the OVERLAPS, tuples, and IS NOT DISTINCT FROM, this query is
    # more gnarly in sqlalchemy than straight SQL, so using SQL instead
    #
    # Why min()?
    # Our data schema doesn't restrict two TAS entries (with the same ATA, AI,
    # etc.) to be disjoint in time, though we do require that there only be a
    # single combination of ATA, AI, etc. and CARS' internal accounting
    # number. In that scenario, we select the minimum of the potential
    # tas_ids. We don't expect this situation to arise in practice, however.
    sql = """
        UPDATE {table_name}
        SET tas_id = (
            SELECT min(tas.tas_id)
            FROM tas_lookup AS tas
            WHERE
            {table_name}.allocation_transfer_agency
                IS NOT DISTINCT FROM tas.allocation_transfer_agency
            AND {table_name}.agency_identifier
                IS NOT DISTINCT FROM tas.agency_identifier
            AND {table_name}.beginning_period_of_availa
                IS NOT DISTINCT FROM tas.beginning_period_of_availability
            AND {table_name}.ending_period_of_availabil
                IS NOT DISTINCT FROM tas.ending_period_of_availability
            AND {table_name}.availability_type_code
                IS NOT DISTINCT FROM tas.availability_type_code
            AND {table_name}.main_account_code
                IS NOT DISTINCT FROM tas.main_account_code
            AND {table_name}.sub_account_code
                IS NOT DISTINCT FROM tas.sub_account_code
            AND (:start_date, :end_date) OVERLAPS
                -- A null end date indicates "still open". To make OVERLAPS
                -- work, we'll use the day after the end date of the
                -- submission to achieve the same result
                (tas.internal_start_date,
                 COALESCE(tas.internal_end_date, :end_date + interval '1 day')
                )
            )
        WHERE submission_id = :submission_id
    """.format(table_name=model.__table__)
    sess.execute(
        sql, {'start_date': submission.reporting_start_date,
              'end_date': submission.reporting_end_date,
              'submission_id': submission_id}
    )
    sess.commit()
