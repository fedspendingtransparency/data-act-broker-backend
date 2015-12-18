import sys, os, inspect, json
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from interfaces.jobTrackerInterface import JobTrackerInterface
import struct
from dataactcore.utils.requestDictionary import RequestDictionary
from fileReaders.csvReader import CsvReader
from interfaces.stagingInterface import StagingInterface
from interfaces.validationInterface import ValidationInterface
from validator import Validator
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.utils.responseException import ResponseException
from csv import Error

class ValidationManager:
    """ Outer level class, called by flask route
    """

    def validateJob(self, request):
        """ Gets file for job, validates each row, and sends valid rows to staging database
        Args:
        request -- HTTP request containing the jobId

        Returns:
        Http response object
        """
        # Create connection to job tracker database

        tableName = ""
        rowNumber = 1
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
            jobTracker = JobTrackerInterface()
            jobTracker.markStatus(jobId,"running")

            # Check that job exists and is ready
            if(not (jobTracker.runChecks(jobId))):
                exc = ResponseException("Checks failed on Job ID")
                exc.status = StatusCode.CLIENT_ERROR
                raise exc

            # Get file type from job tracker
            fileType = jobTracker.getFileType(jobId)

            # Get bucket name and file name
            fileName = jobTracker.getFileName(jobId)
            bucketName = s3UrlHandler.getBucketNameFromConfig()

            validationDB = ValidationInterface()
            fieldList = validationDB.getFieldsByFileList(fileType)
            csvSchema  = validationDB.getFieldsByFile(fileType)
            rules = validationDB.getRulesByFile(fileType)
            # Pull file from S3
            reader = CsvReader()
            # Use test file for now
            #fileName = "test.csv"
            reader.openFile(bucketName, fileName,fieldList)
            # Create staging table
            stagingDb = StagingInterface()
            tableName = stagingDb.createTable(fileType,fileName,jobId,tableName)
            # While not done, pull one row and put it into staging if it passes
            # the Validator
            while(not reader.isFinished):
                rowNumber += 1
                try :
                    record = reader.getNextRecord()
                except ValueError as e:
                    #TODO Logging
                    print("Row " + str(rowNumber) + " failed to get record")
                    continue
                if(Validator.validate(record,rules,csvSchema)) :
                    try:
                        stagingDb.writeRecord(tableName,record)
                    except:
                        # Write failed, move to next record
                        # TODO Logging
                        print("Row " + str(rowNumber) + " failed to write record")
                        continue
                else:
                    #TODO Logging
                    print("Row " + str(rowNumber) + " failed validation")
                    pass

            # Mark validation as finished in job tracker
            jobTracker.markStatus(jobId,"finished")
            return JsonResponse.create(StatusCode.OK,{"table":tableName})
        except ResponseException as e:
            jobTracker.markStatus(jobId,"invalid")
            return JsonResponse.error(e,e.status,{"table":tableName})
        except ValueError as e:
            # Problem with CSV headers
            exc = ResponseException(e.message)
            exc.status = StatusCode.CLIENT_ERROR
            exc.wrappedException = e
            return JsonResponse.error(exc,exc.status,{"table":tableName})
        except Error as e:
            # CSV file not properly formatted (usually too much in one field)
            exc = ResponseException(e.message)
            exc.status = StatusCode.CLIENT_ERROR
            exc.wrappedException = e
            jobTracker.markStatus(jobId,"invalid")
            return JsonResponse.error(exc,exc.status,{"table":tableName})
        except Exception as e:
            exc = ResponseException(e.message)
            exc.wrappedException = e
            jobTracker.markStatus(jobId,"failed")
            return JsonResponse.error(exc,exc.status,{"table":tableName})
