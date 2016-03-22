import requests
import os
import inspect
import time
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.models.jobModels import JobStatus, Submission
from dataactvalidator.models.validationModels import FileColumn

class TestUtils(object):
    """ Basic functions used by validator tests """
    UPLOAD_FILES = True # Upload new csv files to S3, can set to False to skip reuploading same files on subsequent runs
    USE_THREADS = False # If true, each route call launches a new thread
    BASE_URL = "http://127.0.0.1:80"
    #BASE_URL = "http://52.90.92.100:80"
    JSON_HEADER = {"Content-Type": "application/json"}
    LOCAL_FILE_DIRECTORY = "" #This needs to be set to the local dirctory for error reports
    LOCAL = False # True if testing a local installation of the broker

    @staticmethod
    def addJob(status, jobType, submissionId, s3Filename, fileType, session):
        """ Create a job model and add it to the session """
        job = JobStatus(status_id = status, type_id = jobType, submission_id = submissionId, filename = s3Filename, file_type_id = fileType)
        session.add(job)
        session.commit() # Committing immediately so job ID is available
        return job

    @staticmethod
    def addFileColumn(fileId, fieldTypeId, columnName, description, required, session):
        column = FileColumn(file_id = fileId, field_types_id = fieldTypeId, name=columnName, description = description, required = required)
        session.add(column)
        session.commit()
        return column

    @staticmethod
    def insertSubmission(jobTracker):
        """ Insert one submission into job tracker and get submission ID back, uses user_id 1 """
        sub = Submission(datetime_utc = 0, user_id = 1)
        jobTracker.session.add(sub)
        jobTracker.session.commit()
        return sub.submission_id

    @staticmethod
    def uploadFile(filename, user):
        """ Upload file to S3 and return S3 filename"""
        if(len(filename.strip())==0):
            # Empty filename, just return empty
            return ""
        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        fullPath = path + "/" + filename

        if TestUtils.LOCAL:
            # Local version just stores full path in job tracker
            return fullPath
        else:
            # Get bucket name
            bucketName = s3UrlHandler.getValueFromConfig("bucket")

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
            assert(jobTracker.getStatus(jobId) == jobTracker.getStatusId(statusName))

        TestUtils.assertHeader(response)

        if(fileSize != False):
            if TestUtils.LOCAL:
                # TODO: check size of local error reports
                path = "".join([TestUtils.LOCAL_FILE_DIRECTORY,jobTracker.getReportPath(jobId)])
                assert(os.path.getsize(path) > fileSize - 5 )
                assert(os.path.getsize(path) < fileSize + 5 )
            else:
                assert(s3UrlHandler.getFileSize("errors/"+jobTracker.getReportPath(jobId)) > fileSize - 5)
                assert(s3UrlHandler.getFileSize("errors/"+jobTracker.getReportPath(jobId)) < fileSize + 5)

        tableName = response.json()["table"]
        if(type(stagingRows) == type(False) and not stagingRows):
            assert(stagingDb.tableExists(tableName) == False)
        else:
            assert(stagingDb.tableExists(tableName) == True)
            assert(stagingDb.countRows(tableName) == stagingRows)
        errorInterface = interfaces.errorDb
        if(errorStatus is not False):
            assert(errorInterface.checkStatusByJobId(jobId) == errorInterface.getStatusId(errorStatus))
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
        currentID = jobTracker.getStatusId("running")
        targetStatus = jobTracker.getStatusId(status)
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
