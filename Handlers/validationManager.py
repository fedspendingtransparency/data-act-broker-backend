import sys, os, inspect, json
from dataactcore.utils.jsonResponse import JsonResponse
from interfaces.jobTrackerInterface import JobTrackerInterface
import struct
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.responseException import ResponseException
from fileReaders.csvReader import CsvReader
from interfaces.stagingInterface import StagingInterface

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
        try:
            requestDict = RequestDictionary(request)
            if(requestDict.exists("job_id")):
                jobId = requestDict.getValue("job_id")
            else:
                # Request does not have a job ID, can't validate
                exc = ResponseException("No job ID specified in request")
                exc.status = 400
                raise exc
            # Create connection to job tracker database
            jobTracker = JobTrackerInterface()
            # Check that job exists and is ready
            if(not (jobTracker.runChecks(jobId))):
                exc = ResponseException("Checks failed on Job ID")
                exc.status = 400
                raise exc

            # Get file type from job tracker
            fileType = jobTracker.getFileType(jobId)
            print(fileType)

            # Get bucket name and file name
            fileName = jobTracker.getFileName(jobId)
            path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
            bucketFile = open(path+"/s3bucket.json","r").read()
            bucketDict = json.loads(bucketFile)
            bucketName = bucketDict["bucket"]



            # Pull file from S3
            reader = CsvReader()
            print(bucketName)
            print(fileName)
            # Use test file for now
            fileName = "test.csv"
            reader.openFile(bucketName, fileName)
            # Create staging table
            stagingDb = StagingInterface()
            tableName = stagingDb.createTable(fileType,jobId)
            # While not done, pull one row and put it into staging
            record = reader.getNextRecord()
            while(len(record.keys()) > 0):
                stagingDb.writeRecord(tableName,record)
                record = reader.getNextRecord()

            # Mark validation as finished in job tracker
            jobTracker.markFinished(jobId)
            return JsonResponse.create(200,{})
        except ResponseException as e:
            return JsonResponse.error(e,e.status)

if __name__ == '__main__':
    validManager = ValidationManager()
    validManager.validateJob(1)