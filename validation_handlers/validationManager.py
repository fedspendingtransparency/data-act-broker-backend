from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.requestDictionary import RequestDictionary
from filestreaming.csvReader import CsvReader
from validation_handlers.validator import Validator
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.utils.responseException import ResponseException
from csv import Error
from filestreaming.csvWriter import CsvWriter
from validation_handlers.validationError import ValidationError
from interfaces.interfaceHolder import InterfaceHolder
from interfaces.stagingTable import StagingTable

class ValidationManager:
    """

    Outer level class, called by flask route

    """
    reportHeaders = ["Field name", "Error message", "Row number"]

    def __init__(self):
        # Initialize instance variables
        self.filename = ""

    @staticmethod
    def markJob(jobId,jobTracker,status) :
        try :
            jobTracker.markStatus(jobId,status)
        except Exception as e:
            pass

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
            exc = ResponseException("No job ID specified in request")
            exc.status = StatusCode.CLIENT_ERROR
            raise exc

    @staticmethod
    def testJobID(jobId) :
        """
        args
        jobId: job to be tested
        returns the jobId
        True if the job is ready, if the job is not ready an exception will be raised
        """
        tableName = "job"+str(jobId)
        jobTracker = InterfaceHolder.JOB_TRACKER
        if(not (jobTracker.runChecks(jobId))):
            exc = ResponseException("Checks failed on Job ID")
            exc.status = StatusCode.CLIENT_ERROR
            raise exc

        return True


    def threadedValidateJob(self,jobId) :
        """
        args
        jobId -- (Integer) a valid jobId
        This method runs on a new thread thus thus
        there are zero error messages other then the
        job status being updated

        """
        self.filename = ""
        jobTracker = InterfaceHolder.JOB_TRACKER
        try:
            self.runValidation(jobId, jobTracker)
            errorInterface = InterfaceHolder.ERROR
            errorInterface.markFileComplete(jobId,self.filename)
            return
        except ResponseException as e:
            self.markJob(jobId,jobTracker,"invalid")
            errorHandler = InterfaceHolder.ERROR
            errorHandler.writeFileError(jobId,self.filename,e.errorType)
        except ValueError as e:
            self.markJob(jobId,jobTracker,"invalid")
            errorHandler = InterfaceHolder.ERROR
            errorHandler.writeFileError(jobId,self.filename,ValidationError.unknownError)
        except Exception as e:
            #Something unkown happend we may need to try again!
            self.markJob(jobId,jobTracker,"failed")
            errorHandler = InterfaceHolder.ERROR
            errorHandler.writeFileError(jobId,self.filename,ValidationError.unknownError)


    def runValidation(self, jobId, jobTracker):
        rowNumber = 1

        fileType = jobTracker.getFileType(jobId)
        # Get bucket name and file name
        fileName = jobTracker.getFileName(jobId)
        self.filename = fileName
        bucketName = s3UrlHandler.getBucketNameFromConfig()
        errorFileName = jobTracker.getReportPath(jobId)

        validationDB = InterfaceHolder.VALIDATION
        fieldList = validationDB.getFieldsByFileList(fileType)
        csvSchema  = validationDB.getFieldsByFile(fileType)
        rules = validationDB.getRulesByFile(fileType)
        # Pull file from S3
        reader = CsvReader()
        reader.openFile(bucketName, fileName,fieldList)
        # Create staging table
        # While not done, pull one row and put it into staging if it passes
        # the Validator
        tableName = "job"+str(jobId)
        tableObject = StagingTable()
        tableObject.createTable(fileType,fileName,jobId,tableName)
        errorInterface = InterfaceHolder.ERROR

        with CsvWriter(bucketName, errorFileName, self.reportHeaders) as writer:
            while(not reader.isFinished):
                rowNumber += 1
                if (rowNumber % 1000) == 0:
                    print("Validating row " + str(rowNumber))
                try :
                    record = reader.getNextRecord()
                    if(reader.isFinished and len(record) < 2):
                        # This is the last line and is empty, don't record an error
                        break
                except ResponseException as e:
                    if(not (reader.isFinished and reader.extraLine) ) :
                        #Last line may be blank dont throw an error
                        writer.write(["Formatting Error", ValidationError.readErrorMsg, str(rowNumber)])
                        errorInterface.recordRowError(jobId,self.filename,"Formatting Error",ValidationError.readError,rowNumber)
                    continue
                valid, fieldName, error = Validator.validate(record,rules,csvSchema)
                if(valid) :
                    try: 
                        tableObject.insert(record)
                    except ResponseException as e:
                        # Write failed, move to next record
                        writer.write(["Formatting Error", ValidationError.writeErrorMsg, str(rowNumber)])
                        errorInterface.recordRowError(jobId,self.filename,"Formatting Error",ValidationError.writeError,rowNumber)
                        continue

                else:

                    try:
                        # If error is an int, it's one of our prestored messages
                        errorType = int(error)
                        errorMsg = ValidationError.getErrorMessage(errorType)
                    except ValueError:
                        # If not, treat it literally
                        errorMsg = error
                    writer.write([fieldName,errorMsg,str(rowNumber)])
                    errorInterface.recordRowError(jobId,self.filename,fieldName,error,rowNumber)

        # Mark validation as finished in job tracker
        jobTracker.markStatus(jobId,"finished")
        errorInterface.writeAllRowErrors(jobId)
        return True

    def validateJob(self, request):
        """ Gets file for job, validates each row, and sends valid rows to staging database
        Args:
        request -- HTTP request containing the jobId

        Returns:
        Http response object
        """
        # Create connection to job tracker database
        self.filename = None
        tableName = ""
        jobId = None
        jobTracker = InterfaceHolder.JOB_TRACKER

        try:
            requestDict = RequestDictionary(request)
            if(requestDict.exists("job_id")):
                jobId = requestDict.getValue("job_id")
                tableName = "job"+str(jobId)
            else:
                # Request does not have a job ID, can't validate
                exc = ResponseException("No job ID specified in request")
                exc.status = StatusCode.CLIENT_ERROR
                raise exc


            # Check that job exists and is ready
            if(not (jobTracker.runChecks(jobId))):
                exc = ResponseException("Checks failed on Job ID")
                exc.status = StatusCode.CLIENT_ERROR
                raise exc

        except ResponseException as e:
            errorHandler = InterfaceHolder.ERROR
            if(e.errorType == None):
                e.errorType = ValidationError.jobError
            errorHandler.writeFileError(jobId,self.filename,e.errorType)
            return JsonResponse.error(e,e.status,{"table":tableName})
        except Exception as e:
            errorHandler = InterfaceHolder.ERROR
            exc = ResponseException("Internal exception")
            exc.wrappedException = e
            self.markJob(jobId,jobTracker,"failed")
            errorHandler.writeFileError(jobId,self.filename,ValidationError.unknownError)
            return JsonResponse.error(exc,exc.status,{"table":tableName})

        try:
            jobTracker.markStatus(jobId,"running")
            self.runValidation(jobId,jobTracker)
            errorInterface = InterfaceHolder.ERROR
            errorInterface.markFileComplete(jobId,self.filename)
            return  JsonResponse.create(StatusCode.OK,{"table":tableName})
        except ResponseException as e:
            open("errorLog","a").write(e.message)
            self.markJob(jobId,jobTracker,"invalid")
            errorHandler = InterfaceHolder.ERROR
            if(e.errorType == None):
                e.errorType = ValidationError.unknownError
            errorHandler.writeFileError(jobId,self.filename,e.errorType)
            return JsonResponse.error(e,e.status,{"table":tableName})
        except ValueError as e:
            # Problem with CSV headers
            exc = ResponseException("Internal value error")
            exc.status = StatusCode.CLIENT_ERROR
            exc.wrappedException = e
            self.markJob(jobId,jobTracker,"invalid")
            errorHandler = InterfaceHolder.ERROR
            errorHandler.writeFileError(jobId,self.filename,ValidationError.unknownError)
            return JsonResponse.error(exc,exc.status,{"table":tableName})
        except Error as e:
            # CSV file not properly formatted (usually too much in one field)
            exc = ResponseException("Internal error")
            exc.status = StatusCode.CLIENT_ERROR
            exc.wrappedException = e
            self.markJob(jobId,jobTracker,"invalid")
            errorHandler = InterfaceHolder.ERROR
            errorHandler.writeFileError(jobId,self.filename,ValidationError.unknownError)
            return JsonResponse.error(exc,exc.status,{"table":tableName})
        except Exception as e:
            errorHandler = InterfaceHolder.ERROR
            exc = ResponseException("Internal exception")
            exc.wrappedException = e
            self.markJob(jobId,jobTracker,"failed")
            errorHandler.writeFileError(jobId,self.filename,ValidationError.unknownError)
            return JsonResponse.error(exc,exc.status,{"table":tableName})
