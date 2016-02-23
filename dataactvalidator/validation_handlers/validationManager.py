from csv import Error
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactvalidator.filestreaming.csvReader import CsvReader
from dataactvalidator.filestreaming.csvWriter import CsvWriter
from dataactvalidator.validation_handlers.validator import Validator
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder
from dataactvalidator.interfaces.stagingTable import StagingTable

class ValidationManager:
    """

    Outer level class, called by flask route

    """
    reportHeaders = ["Field name", "Error message", "Row number", "Value provided"]

    def __init__(self):
        # Initialize instance variables
        self.filename = ""

    @staticmethod
    def markJob(jobId,jobTracker,status,errorDb,filename = None, fileError = ValidationError.unknownError) :
        """ Update status of a job in job tracker database

        Args:
            jobId: Job to be updated
            jobTracker: Interface object for job tracker
            status: New status for specified job

        """
        try :
            if(filename != None and (status == "invalid" or status == "failed")):
                # Mark the file error that occurred
                errorDb.writeFileError(jobId,filename,fileError)
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
        tableName = "".join(["job",str(jobId)])
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
            open("errorLog","a").write("".join([str(e),"\n"]))
            self.markJob(jobId,jobTracker,"invalid",errorDb,self.filename,e.errorType)
        except ValueError as e:
            open("errorLog","a").write("".join([str(e),"\n"]))
            self.markJob(jobId,jobTracker,"invalid",errorDb,self.filename,ValidationError.unknownError)
        except Exception as e:
            #Something unknown happened we may need to try again!
            open("errorLog","a").write("".join([str(e),"\n"]))
            self.markJob(jobId,jobTracker,"failed",errorDb,self.filename,ValidationError.unknownError)
        finally:
            interfaces.close()

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
        # Get bucket name and file name
        fileName = jobTracker.getFileName(jobId)
        self.filename = fileName
        bucketName = s3UrlHandler.getValueFromConfig("bucket")
        errorFileName = "".join(["errors/",jobTracker.getReportPath(jobId)])

        validationDB = interfaces.validationDb
        fieldList = validationDB.getFieldsByFileList(fileType)
        csvSchema  = validationDB.getFieldsByFile(fileType)
        rules = validationDB.getRulesByFile(fileType)
        # Pull file from S3
        reader = CsvReader()
        reader.openFile(bucketName, fileName,fieldList)
        # Create staging table
        # While not done, pull one row and put it into staging if it passes
        # the Validator
        tableName = "".join(["job",str(jobId)])
        tableObject = StagingTable(interfaces)
        tableObject.createTable(fileType,fileName,jobId,tableName)
        errorInterface = interfaces.errorDb

        with CsvWriter(bucketName, errorFileName, self.reportHeaders) as writer:
            while(not reader.isFinished):
                rowNumber += 1
                #if (rowNumber % 1000) == 0:
                #    print("Validating row " + str(rowNumber))
                try :
                    record = reader.getNextRecord()
                    if(reader.isFinished and len(record) < 2):
                        # This is the last line and is empty, don't record an error
                        break
                except ResponseException as e:
                    if(not (reader.isFinished and reader.extraLine) ) :
                        #Last line may be blank dont throw an error
                        writer.write(["Formatting Error", ValidationError.readErrorMsg, str(rowNumber), ""])
                        errorInterface.recordRowError(jobId,self.filename,"Formatting Error",ValidationError.readError,rowNumber)
                    continue
                valid, failures = Validator.validate(record,rules,csvSchema,fileType,interfaces)
                if(valid) :
                    try:
                        tableObject.insert(record)
                    except ResponseException as e:
                        # Write failed, move to next record
                        writer.write(["Formatting Error", ValidationError.writeErrorMsg, str(rowNumber),""])
                        errorInterface.recordRowError(jobId,self.filename,"Formatting Error",ValidationError.writeError,rowNumber)
                        continue

                else:
                    # For each failure, record it in error report and metadata
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

        # Write leftover records
        tableObject.endBatch()
        # Mark validation as finished in job tracker
        jobTracker.markStatus(jobId,"finished")
        errorInterface.writeAllRowErrors(jobId)
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
                tableName = "".join(["job",str(jobId)])
            else:
                # Request does not have a job ID, can't validate
                raise ResponseException("No job ID specified in request",StatusCode.CLIENT_ERROR)

            # Check that job exists and is ready
            if(not (jobTracker.runChecks(jobId))):
                raise ResponseException("Checks failed on Job ID",StatusCode.CLIENT_ERROR)

        except ResponseException as e:
            open("errorLog","a").write("".join([str(e),"\n"]))
            if(e.errorType == None):
                # Error occurred while trying to get and check job ID
                e.errorType = ValidationError.jobError
            interfaces.errorDb.writeFileError(jobId,self.filename,e.errorType)
            return JsonResponse.error(e,e.status,{"table":tableName})
        except Exception as e:
            open("errorLog","a").write("".join([str(e),"\n"]))
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            self.markJob(jobId,jobTracker,"failed",interfaces.errorDb,self.filename,ValidationError.unknownError)
            return JsonResponse.error(exc,exc.status,{"table":tableName})

        try:
            jobTracker.markStatus(jobId,"running")
            self.runValidation(jobId,interfaces)
            interfaces.errorDb.markFileComplete(jobId,self.filename)
            return  JsonResponse.create(StatusCode.OK,{"table":tableName})
        except ResponseException as e:
            open("errorLog","a").write("".join([str(e),"\n"]))
            self.markJob(jobId,jobTracker,"invalid",interfaces.errorDb,self.filename,e.errorType)
            return JsonResponse.error(e,e.status,{"table":tableName})
        except ValueError as e:
            open("errorLog","a").write("".join([str(e),"\n"]))
            # Problem with CSV headers
            exc = ResponseException("Internal value error",StatusCode.CLIENT_ERROR,type(e),ValidationError.unknownError)
            self.markJob(jobId,jobTracker,"invalid",interfaces.errorDb,self.filename,ValidationError.unknownError)
            return JsonResponse.error(exc,exc.status,{"table":tableName})
        except Error as e:
            open("errorLog","a").write("".join([str(e),"\n"]))
            # CSV file not properly formatted (usually too much in one field)
            exc = ResponseException("Internal error",StatusCode.CLIENT_ERROR,type(e),ValidationError.unknownError)
            self.markJob(jobId,jobTracker,"invalid",interfaces.errorDb,self.filename,ValidationError.unknownError)
            return JsonResponse.error(exc,exc.status,{"table":tableName})
        except Exception as e:
            open("errorLog","a").write("".join([str(e),"\n"]))
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e),ValidationError.unknownError)
            self.markJob(jobId,jobTracker,"failed",interfaces.errorDb,self.filename,ValidationError.unknownError)
            return JsonResponse.error(exc,exc.status,{"table":tableName})
