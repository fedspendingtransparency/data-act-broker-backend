import requests
import os
import inspect
import time
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.models import errorModels
from dataactcore.models.jobModels import Status
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder

class TestUtils(object):
    """ Basic functions used by validator tests """
    UPLOAD_FILES = True # Upload new csv files to S3, can set to False to skip reuploading same files on subsequent runs
    USE_THREADS = False # If true, each route call launches a new thread
    BASE_URL = "http://127.0.0.1:80"
    #BASE_URL = "http://52.90.92.100:80"
    JSON_HEADER = {"Content-Type": "application/json"}

    @staticmethod
    def createJobStatement(status, type, submission, s3Filename, fileType):
        """ Build SQL statement to create a job  """
        return "INSERT INTO job_status (status_id, type_id, submission_id, filename, file_type_id) VALUES (" + status + "," + type + "," + submission + ", '" + s3Filename + "',"+ fileType +") RETURNING job_id"

    @staticmethod
    def createColumnStatement(file_id, field_type, columnName, description, required):
        return "INSERT INTO file_columns (file_id,field_types_id,name,description,required) VALUES (" + str(file_id) + ", " + str(field_type) + ", '" + columnName + "', '" + description + "', " + str(required) + ") RETURNING file_column_id"

    @staticmethod
    def insertSubmission(jobTracker):
        """ Insert one submission into job tracker and get submission ID back, uses user_id 1 """
        stmt = "INSERT INTO submission (datetime_utc,user_id) VALUES (0,1) RETURNING submission_id"
        response = jobTracker.runStatement(stmt)
        return response.fetchone()[0]

    @staticmethod
    def uploadFile(filename, user):
        """ Upload file to S3 and return S3 filename"""
        if(len(filename.strip())==0):
            # Empty filename, just return empty
            return ""

        # Get bucket name
        bucketName = s3UrlHandler.getValueFromConfig("bucket")

        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        fullPath = path + "/" + filename

        # Create file names for S3
        s3FileName = str(user) + "/" + filename

        if(TestUtils.UPLOAD_FILES) :
            # Use boto to put files on S3
            s3conn = S3Connection()
            key = Key(s3conn.get_bucket(bucketName))
            key.key = s3FileName
            bytesWritten = key.set_contents_from_filename(fullPath)

            assert(bytesWritten > 0)
        return s3FileName

    @staticmethod
    def run_test(jobId, statusId,statusName,fileSize,stagingRows,errorStatus,numErrors,testCase):
        interfaces = testCase.interfaces
        response = TestUtils.validateJob(jobId)
        testCase.response = response # Set the response in the calling test case for error display
        jobTracker = interfaces.jobDb
        stagingDb = interfaces.stagingDb
        assert(response.status_code == statusId)
        if(statusName != False):
            TestUtils.waitOnJob(jobTracker, jobId, statusName)
            assert(jobTracker.getStatus(jobId) == Status.getStatus(statusName))

        TestUtils.assertHeader(response)

        if(fileSize != False):
            assert(s3UrlHandler.getFileSize("errors/"+jobTracker.getReportPath(jobId)) > fileSize - 5)
            assert(s3UrlHandler.getFileSize("errors/"+jobTracker.getReportPath(jobId)) < fileSize + 5)

        tableName = response.json()["table"]
        if(type(stagingRows) == type(False) and not stagingRows):
            assert(stagingDb.tableExists(tableName) == False)
        else:
            assert(stagingDb.tableExists(tableName) == True)
            assert(stagingDb.countRows(tableName) == stagingRows)
        errorInterface = interfaces.errorDb
        assert(errorInterface.checkStatusByJobId(jobId) == errorModels.Status.getStatus(errorStatus))
        assert(errorInterface.checkNumberOfErrorsByJobId(jobId) == numErrors)
        return True

    @staticmethod
    def assertHeader(response):
        """ Assert that content type header exists and is json """
        assert("Content-Type" in response.headers)
        assert(response.headers["Content-Type"] == "application/json")

    @staticmethod
    def waitOnJob(jobTracker, jobId, status):
        """ Wait until job gets set to the correct status in job tracker, this is done to wait for validation to complete when running tests """
        currentID = Status.getStatus("running")
        targetStatus = Status.getStatus(status)
        if TestUtils.USE_THREADS:
            while jobTracker.getStatus(jobId) == currentID:
                time.sleep(1)
            assert(targetStatus == jobTracker.getStatus(jobId))
        else:
            assert(targetStatus == jobTracker.getStatus(jobId))
            return

    @staticmethod
    def validateJob(jobId):
        """ Send request to validate specified job """
        if TestUtils.USE_THREADS:
            url = "/validate_threaded/"
        else:
            url = "/validate/"
        return requests.request(method="POST", url=TestUtils.BASE_URL + url, data=TestUtils.jobJson(jobId), headers=TestUtils.JSON_HEADER)

    @staticmethod
    def jobJson(jobId):
        """ Create JSON to hold jobId """
        return '{"job_id":'+str(jobId)+'}'