import os
import traceback
import sys
from csv import Error
from dataactcore.config import CONFIG_BROKER
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
from dataactvalidator.interfaces.stagingTable import StagingTable


class ValidationManager:
    """
    Outer level class, called by flask route
    """
    reportHeaders = ["Field name", "Error message", "Row number", "Value provided"]

    def __init__(self,isLocal =True,directory=""):
        # Initialize instance variables
        self.filename = ""
        self.isLocal = isLocal
        self.directory = directory

    @staticmethod
    def markJob(jobId,jobTracker,status,errorDb,filename = None, fileError = ValidationError.unknownError, extraInfo = None) :
        """ Update status of a job in job tracker database
        Args:
            jobId: Job to be updated
            jobTracker: Interface object for job tracker
            status: New status for specified job
        """
        try :
            if(filename != None and (status == "invalid" or status == "failed")):
                # Mark the file error that occurred
                errorDb.writeFileError(jobId,filename,fileError,extraInfo)
            jobTracker.markStatus(jobId,status)
        except ResponseException as e:
            # Could not get a unique job ID in the database, either a bad job ID was passed in or the record of that job was lost.
            # Either way, cannot mark status of a job that does not exist
            open("databaseErrors.log","a").write("".join(["Could not mark status ",str(status)," for job ID ",str(jobId),"\n"]))

    @staticmethod
    def getJobID(request):
        """ Pull job ID out of request
        Args:
            request: HTTP request containing the job ID
        Returns:
            job ID, or raises exception if job ID not found in request
        """
        requestDict = RequestDictionary(request)
        if(requestDict.exists("job_id")):
            jobId = requestDict.getValue("job_id")
            return jobId
        else:
                # Request does not have a job ID, can't validate
            raise ResponseException("No job ID specified in request",StatusCode.CLIENT_ERROR)

    @staticmethod
    def testJobID(jobId,interfaces) :
        """
        args
        jobId: job to be tested
        returns the jobId
        True if the job is ready, if the job is not ready an exception will be raised
        """
        if(not (interfaces.jobDb.runChecks(jobId))):
            raise ResponseException("Checks failed on Job ID",StatusCode.CLIENT_ERROR)

        return True


    def threadedValidateJob(self,jobId) :
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
            self.runValidation(jobId, interfaces)
            errorDb.markFileComplete(jobId,self.filename)
            return
        except ResponseException as e:
            CloudLogger.logError(str(e),e,traceback.extract_tb(sys.exc_info()[2]))
            self.markJob(jobId,jobTracker,"invalid",errorDb,self.filename,e.errorType,e.extraInfo)
        except ValueError as e:
            CloudLogger.logError(str(e),e,traceback.extract_tb(sys.exc_info()[2]))
            self.markJob(jobId,jobTracker,"invalid",errorDb,self.filename,ValidationError.unknownError)
        except Exception as e:
            #Something unknown happened we may need to try again!
            CloudLogger.logError(str(e),e,traceback.extract_tb(sys.exc_info()[2]))
            self.markJob(jobId,jobTracker,"failed",errorDb,self.filename,ValidationError.unknownError)
        finally:
            interfaces.close()

    def getReader(self):
        """
        Gets the reader type based on if its local install or not.
        """
        if(self.isLocal):
            return CsvLocalReader()
        return CsvS3Reader()

    def getWriter(self,regionName,bucketName,fileName,header):
        """
        Gets the write type based on if its a local install or not.
        """
        if(self.isLocal):
            return CsvLocalWriter(fileName,header)
        return CsvS3Writer(regionName,bucketName,fileName,header)

    def getFileName(self,path):
        if(self.isLocal):
            return "".join([self.directory,path])
        return "".join(["errors/",path])

    def runValidation(self, jobId, interfaces):
        """ Run validations for specified job
        Args:
            jobId: Job to be validated
            jobTracker: Interface for job tracker
        Returns:
            True if successful
        """
        jobTracker = interfaces.jobDb
        rowNumber = 1
        fileType = jobTracker.getFileType(jobId)
        # If local, make the error report directory
        if(self.isLocal and not os.path.exists(self.directory)):
            os.makedirs(self.directory)
        # Get bucket name and file name
        fileName = jobTracker.getFileName(jobId)
        self.filename = fileName
        bucketName = CONFIG_BROKER['aws_bucket']
        regionName = CONFIG_BROKER['aws_region']

        errorFileName = self.getFileName(jobTracker.getReportPath(jobId))

        # Create File Status object
        interfaces.errorDb.createFileStatusIfNeeded(jobId,fileName)

        validationDB = interfaces.validationDb
        fieldList = validationDB.getFieldsByFileList(fileType)
        csvSchema  = validationDB.getFieldsByFile(fileType)
        rules = validationDB.getRulesByFile(fileType)

        reader = self.getReader()

        # Get file size and write to jobs table
        if(CONFIG_BROKER["use_aws"]):
            fileSize =  s3UrlHandler.getFileSize("errors/"+jobTracker.getReportPath(jobId))
        else:
            fileSize = os.path.getsize(jobTracker.getFileName(jobId))
        jobTracker.setFileSizeById(jobId, fileSize)


        try:
            # Pull file
            reader.openFile(regionName, bucketName, fileName,fieldList,bucketName,errorFileName)
            # Create staging table
            # While not done, pull one row and put it into staging if it passes
            # the Validator

            tableName = interfaces.stagingDb.getTableName(jobId)
            # Create staging table
            tableObject = StagingTable(interfaces)
            tableObject.createTable(fileType,fileName,jobId,tableName)
            errorInterface = interfaces.errorDb

            # While not done, pull one row and put it into staging if it passes
            # the Validator
            with self.getWriter(regionName, bucketName, errorFileName, self.reportHeaders) as writer:
                while(not reader.isFinished):
                    rowNumber += 1
                    #if (rowNumber % 1000) == 0:
                    #    print("Validating row " + str(rowNumber))
                    try :
                        record = reader.getNextRecord()
                        if(reader.isFinished and len(record) < 2):
                            # This is the last line and is empty, don't record an error
                            rowNumber -= 1 # Don't count this row
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
                    valid, failures = Validator.validate(record,rules,csvSchema,fileType,interfaces)
                    if(valid) :
                        try:
                            tableObject.insert(record,fileType)
                        except ResponseException as e:
                            # Write failed, move to next record
                            writer.write(["Formatting Error", ValidationError.writeErrorMsg, str(rowNumber),""])
                            errorInterface.recordRowError(jobId,self.filename,"Formatting Error",ValidationError.writeError,rowNumber)
                            errorInterface.setRowErrorsPresent(jobId, True)
                            continue

                    else:
                        # For each failure, record it in error report and metadata
                        if failures:
                            errorInterface.setRowErrorsPresent(jobId, True)
                        for failure in failures:
                            fieldName = failure[0]
                            error = failure[1]
                            failedValue = failure[2]
                            try:
                                # If error is an int, it's one of our prestored messages
                                errorType = int(error)
                                errorMsg = ValidationError.getErrorMessage(errorType)
                            except ValueError:
                                # If not, treat it literally
                                errorMsg = error
                            writer.write([fieldName,errorMsg,str(rowNumber),failedValue])
                            errorInterface.recordRowError(jobId,self.filename,fieldName,error,rowNumber)
                # Write unfinished batch
                writer.finishBatch()

            # Write number of rows to job table
            jobTracker.setNumberOfRowsById(jobId,rowNumber)
            # Write leftover records
            tableObject.endBatch()
            # Mark validation as finished in job tracker
            jobTracker.markStatus(jobId,"finished")
            errorInterface.writeAllRowErrors(jobId)
        finally:
            #ensure the file always closes
            reader.close()
        return True

    def validateJob(self, request,interfaces):
        """ Gets file for job, validates each row, and sends valid rows to staging database
        Args:
        request -- HTTP request containing the jobId
        sessions -- A SessionHolder object used to query the databases
        Returns:
        Http response object
        """
        # Create connection to job tracker database
        self.filename = None
        tableName = ""
        jobId = None
        jobTracker = None

        try:
            jobTracker = interfaces.jobDb
            requestDict = RequestDictionary(request)
            if(requestDict.exists("job_id")):
                jobId = requestDict.getValue("job_id")
                tableName = interfaces.stagingDb.getTableName(jobId)
            else:
                # Request does not have a job ID, can't validate
                raise ResponseException("No job ID specified in request",StatusCode.CLIENT_ERROR)

            # Check that job exists and is ready
            if(not (jobTracker.runChecks(jobId))):
                raise ResponseException("Checks failed on Job ID",StatusCode.CLIENT_ERROR)

        except ResponseException as e:
            CloudLogger.logError(str(e),e,traceback.extract_tb(sys.exc_info()[2]))
            if(e.errorType == None):
                # Error occurred while trying to get and check job ID
                e.errorType = ValidationError.jobError
            interfaces.errorDb.writeFileError(jobId,self.filename,e.errorType,e.extraInfo)
            return JsonResponse.error(e,e.status,table=tableName)
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            CloudLogger.logError(str(e),e,traceback.extract_tb(sys.exc_info()[2]))
            self.markJob(jobId,jobTracker,"failed",interfaces.errorDb,self.filename,ValidationError.unknownError)
            return JsonResponse.error(exc,exc.status,table=tableName)

        try:
            jobTracker.markStatus(jobId,"running")
            self.runValidation(jobId,interfaces)
            interfaces.errorDb.markFileComplete(jobId,self.filename)
            return  JsonResponse.create(StatusCode.OK,{"table":tableName})
        except ResponseException as e:
            CloudLogger.logError(str(e),e,traceback.extract_tb(sys.exc_info()[2]))
            self.markJob(jobId,jobTracker,"invalid",interfaces.errorDb,self.filename,e.errorType,e.extraInfo)
            return JsonResponse.error(e,e.status,table=tableName)
        except ValueError as e:
            CloudLogger.logError(str(e),e,traceback.extract_tb(sys.exc_info()[2]))
            # Problem with CSV headers
            exc = ResponseException(str(e),StatusCode.CLIENT_ERROR,type(e),ValidationError.unknownError) #"Internal value error"
            self.markJob(jobId,jobTracker,"invalid",interfaces.errorDb,self.filename,ValidationError.unknownError)
            return JsonResponse.error(exc,exc.status,table=tableName)
        except Error as e:
            CloudLogger.logError(str(e),e,traceback.extract_tb(sys.exc_info()[2]))
            # CSV file not properly formatted (usually too much in one field)
            exc = ResponseException("Internal error",StatusCode.CLIENT_ERROR,type(e),ValidationError.unknownError)
            self.markJob(jobId,jobTracker,"invalid",interfaces.errorDb,self.filename,ValidationError.unknownError)
            return JsonResponse.error(exc,exc.status,table=tableName)
        except Exception as e:
            CloudLogger.logError(str(e),e,traceback.extract_tb(sys.exc_info()[2]))
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e),ValidationError.unknownError)
            self.markJob(jobId,jobTracker,"failed",interfaces.errorDb,self.filename,ValidationError.unknownError)
            return JsonResponse.error(exc,exc.status,table=tableName)