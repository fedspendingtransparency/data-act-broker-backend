import json
import unittest
from handlers.managerProxy import ManagerProxy
from test.baseTest import BaseTest
from test.testUtils import TestUtils
from dataactcore.scripts.createJobTables import createJobTables
from dataactcore.scripts.clearJobs import clearJobs
from handlers.interfaceHolder import InterfaceHolder
from dataactcore.aws.s3UrlHandler import s3UrlHandler
import os
import inspect
from boto.s3.connection import S3Connection
from boto.s3.key import Key

class FileTests(BaseTest):
    """ Test file submission routes """
    fileResponse = None
    CHECK_ERROR_REPORTS = False
    CHECK_VALIDATOR = True
    JOB_ID_FILE = "jobId.json"
    SUBMISSION_ID_FILE = "submissionId"
    TABLES_CLEARED_FILE = "tablesCleared" # Holds a boolean flag in a file so it can be checked before doing table setup
    tablesCleared = False # Set to true after first time setup is run
    submissionId = None

    def __init__(self,methodName):
        """ Run scripts to clear the job tables and populate with a defined test set """
        super(FileTests,self).__init__(methodName=methodName)
        jobTracker = InterfaceHolder.JOB_TRACKER
        try:
            self.tablesCleared = self.toBool(open(self.TABLES_CLEARED_FILE,"r").read())
        except Exception as e:
            # Could not read from file or cast as boolean
            print(e.message)
            self.tablesCleared = False

        if(not self.tablesCleared):
            # Clear job tracker
            #clearJobs()
            self.tablesCleared = True
            open(self.TABLES_CLEARED_FILE,"w").write(str(True))
            # Create submission ID
            submissionResponse = jobTracker.runStatement("INSERT INTO submission (datetime_utc) VALUES (0) RETURNING submission_id")
            submissionId = submissionResponse.fetchone()[0]
            self.submissionId = submissionId
            # Create jobs
            sqlStatements = ["INSERT INTO job_status (file_type_id, status_id, type_id, submission_id) VALUES (1,4,1,"+str(submissionId)+") RETURNING job_id",
                             "INSERT INTO job_status (file_type_id, status_id, type_id, submission_id) VALUES (1,3,2,"+str(submissionId)+") RETURNING job_id",
                             "INSERT INTO job_status (file_type_id, status_id, type_id, submission_id) VALUES (1,1,5,"+str(submissionId)+") RETURNING job_id",
                             "INSERT INTO job_status (file_type_id, status_id, type_id, submission_id) VALUES (2,2,2,"+str(submissionId)+") RETURNING job_id",
                             "INSERT INTO job_status (file_type_id, status_id, type_id, submission_id) VALUES (3,2,2,"+str(submissionId)+") RETURNING job_id",
                             "INSERT INTO job_status (file_type_id, status_id, type_id, submission_id) VALUES (4,2,2,"+str(submissionId)+") RETURNING job_id"]

            jobKeyList = ["uploadFinished","recordRunning","externalWaiting","awardFin","appropriations","procurement"]
            index = 0
            self.jobIdDict = {}
            for statement in sqlStatements:
                self.jobIdDict[jobKeyList[index]] = jobTracker.runStatement(statement).fetchone()[0]
                index += 1
            # Save jobIdDict to file
            print("jobIdDict is " + str(self.jobIdDict))
            open(self.JOB_ID_FILE,"w").write(json.dumps(self.jobIdDict))
            open(self.SUBMISSION_ID_FILE,"w").write(str(submissionId))
        else:
            # Read job ID dict from file
            self.jobIdDict = json.loads(open(self.JOB_ID_FILE,"r").read())
            self.submissionId = int(open(self.SUBMISSION_ID_FILE,"r").read())

    @staticmethod
    def toBool(stringValue):
        if(stringValue.lower() == "true"):
            return True
        elif(stringValue.lower() == "false"):
            return False
        else:
            raise ValueError("Invalid string passed to toBool function")

    def call_file_submission(self):
        # If fileResponse doesn't exist, send the request
        fileJson = '{"appropriations":"test1.csv","award_financial":"test2.csv","award":"test3.csv","procurement":"test4.csv"}'
        if(self.fileResponse == None):
            self.utils.login()
            self.fileResponse = self.utils.postRequest("/v1/submit_files/",fileJson)

    def test_file_submission(self):
        self.call_file_submission()

        # Test that status is 200
        assert(self.fileResponse.status_code==200)
        # Test Content-Type header
        assert("Content-Type" in self.fileResponse.headers)
        assert(self.fileResponse.headers["Content-Type"]=="application/json")
        # Test message parts for urls
        assert("_test1.csv" in self.fileResponse.json()["appropriations_url"] )
        assert("_test2.csv" in self.fileResponse.json()["award_financial_url"])
        assert("_test3.csv" in self.fileResponse.json()["award_url"])
        assert("_test4.csv" in self.fileResponse.json()["procurement_url"])
        self.uploadFileSigned(self.fileResponse.json()["procurement_url"],"test4.csv")
        assert("?Signature" in self.fileResponse.json()["appropriations_url"] )
        assert("?Signature" in self.fileResponse.json()["award_financial_url"])
        assert("?Signature" in self.fileResponse.json()["award_url"])
        assert("?Signature" in self.fileResponse.json()["procurement_url"])
        assert("&AWSAccessKeyId" in self.fileResponse.json()["appropriations_url"] )
        assert("&AWSAccessKeyId" in self.fileResponse.json()["award_financial_url"])
        assert("&AWSAccessKeyId" in self.fileResponse.json()["award_url"])
        assert("&AWSAccessKeyId" in self.fileResponse.json()["procurement_url"])
        # Test that job ids are returned
        responseDict = self.fileResponse.json()
        idKeys = ["procurement_id", "award_id", "award_financial_id", "appropriations_id"]
        for key in idKeys:
            assert(key in responseDict)
            try:
                int(responseDict[key])
            except:
                self.fail("One of the job ids returned was not an integer")
            # Call upload complete route for each id
        self.check_upload_complete(responseDict["procurement_id"])
        #self.check_error_route (responseDict["procurement_id"],responseDict["submission_id"])
        if(self.CHECK_VALIDATOR):
            self.check_validator(responseDict["procurement_id"])

    def test_check_status(self):
        """ Check that test status route returns correct JSON"""
        utils = TestUtils()
        utils.login()
        response = utils.postRequest("/v1/check_status/",'{"submission_id":'+str(self.submissionId)+'}')

        assert(response.status_code == 200)
        assert(response.json()[str(self.jobIdDict["uploadFinished"])]["status"]=="finished")
        assert(response.json()[str(self.jobIdDict["uploadFinished"])]["job_type"]=="file_upload")
        assert(response.json()[str(self.jobIdDict["uploadFinished"])]["file_type"]=="award")
        assert(response.json()[str(self.jobIdDict["recordRunning"])]["status"]=="running")
        assert(response.json()[str(self.jobIdDict["recordRunning"])]["job_type"]=="csv_record_validation")
        assert(response.json()[str(self.jobIdDict["recordRunning"])]["file_type"]=="award")
        assert(response.json()[str(self.jobIdDict["externalWaiting"])]["status"]=="waiting")
        assert(response.json()[str(self.jobIdDict["externalWaiting"])]["job_type"]=="external_validation")
        assert(response.json()[str(self.jobIdDict["externalWaiting"])]["file_type"]=="award")
        assert(response.json()[str(self.jobIdDict["appropriations"])]["status"]=="ready")
        assert(response.json()[str(self.jobIdDict["appropriations"])]["job_type"]=="csv_record_validation")
        assert(response.json()[str(self.jobIdDict["appropriations"])]["file_type"]=="appropriations")

    def check_error_route(self,jobId,submissonId) :
        jobJson = json.dumps({"upload_id":jobId})
        urlData = self.utils.postRequest("/v1/job_error_report/",jobJson)
        assert("submission_"+str(submissonId)+"_procurement_error_report" in urlData.json()["error_url"] )
        assert("?Signature" in urlData.json()["error_url"] )
        assert("&AWSAccessKeyId" in urlData.json()["error_url"])

    def check_upload_complete(self, jobId):
        jobJson = json.dumps({"upload_id":jobId})
        self.utils.login()
        finalizeResponse = self.utils.postRequest("/v1/finalize_job/",jobJson)

        assert(finalizeResponse.status_code == 200)

    def check_validator(self, jobId):
        proxy = ManagerProxy()
        # Get the ID of the validation job dependent on this upload job
        self.uploadFile("test4.csv","1")
        jobTracker = InterfaceHolder.JOB_TRACKER
        validationId = jobTracker.getDependentJobs(jobId)
        assert(len(validationId) == 1) # Should only be one job directly dependent on upload
        self.response = proxy.sendJobRequest(validationId[0])
        assert(self.response.status_code == 400)

    @staticmethod
    def uploadFileSigned(s3Url, filename):
        """ Upload file to signed S3 URL and return True if successful"""
        utils = TestUtils()
        response = utils.postRequest(s3Url,open(filename,"r").read(),{"Content-Type":"application/octet-stream"},True,"PUT")
        assert(response.status_code == 200)
        return True

    @staticmethod
    def uploadFile(filename, user, s3FileName = None):
        """ Upload file to S3 and return S3 filename"""
        # Get bucket name
        bucketName = s3UrlHandler.getBucketNameFromConfig()

        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        fullPath = path + "/" + filename

        if(s3FileName == None):
            # Create file names for S3
            s3FileName = str(user) + "/" + filename


        # Use boto to put files on S3
        s3conn = S3Connection()
        key = Key(s3conn.get_bucket(bucketName))
        key.key = s3FileName
        bytesWritten = key.set_contents_from_filename(fullPath)

        assert(bytesWritten > 0)
        return s3FileName

    def test_error_report(self):
        # Will only pass if specific submission ID is entered after validator unit tests have been run to generate the error reports
        if not self.CHECK_ERROR_REPORTS:
            # Skip error report test
            return
        utils = TestUtils()
        utils.login()

        self.setupJobsForReports()
        response = utils.postRequest("/v1/submission_error_reports/",'{"submission_id":11}')
        clearJobs()  # Clear job DB again so sequence errors don't occur
        assert(response.status_code == 200)
        assert(len(response.json()) == 4)

    def setupJobsForReports(self):
        """ Setting Jobs table to correct state for checking error reports from validator unit tests """
        clearJobs()
        self.tablesCleared = False
        sqlStatements = [
            "INSERT INTO submission (submission_id,datetime_utc) VALUES (11,0)",
            "INSERT INTO job_status (job_id,file_type_id, status_id, type_id, submission_id) VALUES (11,1,4,2,11),(12,2,4,2,11),(13,3,4,2,11),(15,4,4,2,11)"
        ]

        jobTracker = InterfaceHolder.JOB_TRACKER
        for statement in sqlStatements:
            jobTracker.runStatement(statement)

if __name__ == '__main__':
    unittest.main()
