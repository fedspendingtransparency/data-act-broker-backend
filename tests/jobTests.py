import unittest
from dataactcore.models.stagingInterface import StagingInterface
from dataactcore.models.jobModels import JobStatus, JobDependency, Status, Type
import requests
from interfaces.jobTrackerInterface import JobTrackerInterface
import os
import inspect
from dataactcore.aws.s3UrlHandler import s3UrlHandler
import json
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from dataactcore.scripts.databaseSetup import runCommands

class JobTests(unittest.TestCase):
    BASE_URL = "http://127.0.0.1:5000"
    JSON_HEADER = {"Content-Type": "application/json"}
    TABLE_POPULATED = False # Gets set to true by the first test to populate the tables

    def __init__(self,methodName):
        """ Run scripts to clear the job tables and populate with a defined test set """
        super(JobTests,self).__init__(methodName=methodName)
        if(not self.TABLE_POPULATED):
            # Create staging database
            try:
                runCommands(StagingInterface.getCredDict(),[],"staging")
            except:
                # Staging database already exists, keep going
                pass

            # Clear job tables
            import dataactcore.scripts.clearJobs

            # Get bucket name
            bucketName = s3UrlHandler.getBucketNameFromConfig()
            user = 1
            s3manager = s3UrlHandler(bucketName,user)
            # Upload needed files to S3

            fnameValid = "testValid.csv"
            fnamePrereq = "testPrereq.csv"
            urlValid = s3manager.getSignedUrl(fnameValid)
            urlPrereq = s3manager.getSignedUrl(fnamePrereq)
            path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
            fullPathValid = path + "/" +fnameValid
            fullPathPrereq = path + "/" + fnamePrereq
            fileValid = open(fullPathValid, 'rb')
            filePrereq = open(fullPathPrereq, 'rb')

            # Create file names for S3
            s3FileNameValid = str(user) + "/" + fnameValid
            s3FileNamePrereq = str(user) + "/" + fnamePrereq

            # Use boto to put files on S3
            s3conn = S3Connection()
            keyValid = Key(s3conn.get_bucket(bucketName))
            keyValid.key = s3FileNameValid
            validWritten = keyValid.set_contents_from_filename(fullPathValid)
            keyPrereq = Key(s3conn.get_bucket(bucketName))
            keyPrereq.key = s3FileNamePrereq
            prereqWritten = keyPrereq.set_contents_from_filename(fullPathPrereq)
            assert(validWritten > 0)
            assert(prereqWritten > 0)

            # Populate with a defined test set
            jobTracker = JobTrackerInterface()
            sqlStatements = ["INSERT INTO submission (submission_id) VALUES (1)",
            "INSERT INTO job_status (job_id, status_id, type_id, submission_id, filename, file_type_id) VALUES (1, " + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ",1, '" + s3FileNameValid + "',1)",
            "INSERT INTO job_status (job_id, status_id, type_id, submission_id, file_type_id) VALUES (2, " + str(Status.getStatus("ready")) + "," + str(Type.getType("file_upload")) + ",1,1)",
            "INSERT INTO job_status (job_id, status_id, type_id, submission_id, file_type_id) VALUES (3, " + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ",1,1)",
            "INSERT INTO job_dependency (dependency_id, job_id, prerequisite_id) VALUES (1, 3, 2)",
            "INSERT INTO job_status (job_id, status_id, type_id, submission_id, file_type_id) VALUES (4, " + str(Status.getStatus("ready")) + "," + str(Type.getType("external_validation")) + ",1,1)",
            "INSERT INTO job_status (job_id, status_id, type_id, submission_id, file_type_id) VALUES (5, " + str(Status.getStatus("finished")) + "," + str(Type.getType("csv_record_validation")) + ",1,1)",
            "INSERT INTO job_status (job_id, status_id, type_id, submission_id, file_type_id) VALUES (6, " + str(Status.getStatus("finished")) + "," + str(Type.getType("file_upload")) + ",1,1)",
            "INSERT INTO job_status (job_id, status_id, type_id, submission_id, filename, file_type_id) VALUES (7, " + str(Status.getStatus("ready")) + "," + str(Type.getType("csv_record_validation")) + ",1, '" + s3FileNamePrereq + "',1)",
            "INSERT INTO job_dependency (dependency_id, job_id, prerequisite_id) VALUES (2, 7, 6)",
            ]
            for statement in sqlStatements:
                jobTracker.runStatement(statement)
            JobTests.TABLE_POPULATED = True


    def test_valid_job(self):
        """ Test valid job """

        validResponse = self.validateJob(1)
        if(validResponse.status_code != 200):
            print(validResponse.status_code)
            print(validResponse.json()["errorType"])
            print(validResponse.json()["message"])
            print(validResponse.json()["trace"])
        assert(validResponse.status_code == 200)
        self.assertHeader(validResponse)
        # Check that job is correctly marked as finished
        jobTracker = JobTrackerInterface()
        assert(jobTracker.getStatus(1) == Status.getStatus("finished"))

    def test_bad_id_job(self):
        """ Test job ID not found in job status table """
        badIdResponse = self.validateJob(2001)
        assert(badIdResponse.status_code == 400)
        self.assertHeader(badIdResponse)
        assert(badIdResponse.json()["message"]=="Job ID not found in job_status table")

    def test_prereq_job(self):
        """ Test job with prerequisites finished """
        prereqResponse = self.validateJob(7)
        assert(prereqResponse.status_code == 200)
        self.assertHeader(prereqResponse)

    def test_bad_prereq_job(self):
        """ Test job with unfinished prerequisites """
        prereqResponse = self.validateJob(3)
        assert(prereqResponse.status_code == 400)
        self.assertHeader(prereqResponse)
        assert(prereqResponse.json()["message"] == "Prerequisites incomplete, job cannot be started")

    def test_bad_type_job(self):
        """ Test job with wrong type """
        badTypeResponse = self.validateJob(4)
        assert(badTypeResponse.status_code == 400)
        self.assertHeader(badTypeResponse)
        assert(badTypeResponse.json()["message"] == "Wrong type of job for this service")

    # TODO uncomment this unit test once jobs are labeled as ready
    #def test_finished_job(self):
        #""" Test job that is already finished """
        #finishedResponse = self.validateJob(5)
        #assert(finishedResponse.status_code == 400)
        #self.assertHeader(finishedResponse)
        #assert(finishedResponse.json()["message"] == "Job is not ready")

    def assertHeader(self, response):
        """ Assert that content type header exists and is json """
        assert("Content-Type" in response.headers)
        assert(response.headers["Content-Type"] == "application/json")

    def validateJob(self, jobId):
        """ Send request to validate specified job """
        url = "/validate/"
        return requests.request(method="POST", url=self.BASE_URL + url, data=self.jobJson(jobId), headers = self.JSON_HEADER)

    def jobJson(self,jobId):
        """ Create JSON to hold jobId """
        return '{"job_id":'+str(jobId)+'}'