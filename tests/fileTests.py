import json
import unittest
import os
import inspect
import time
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from baseTest import BaseTest
from testUtils import TestUtils
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.models.jobModels import Submission, JobStatus
from dataactcore.models.errorModels import ErrorData, FileStatus
from dataactbroker.handlers.fileHandler import FileHandler

class FileTests(BaseTest):
    """ Test file submission routes """
    fileResponse = None
    CHECK_ERROR_REPORTS = False # If True, must provide submission ID below
    ERROR_REPORT_SUBMISSION_ID = 11
    JOB_ID_FILE = "jobId.json"
    SUBMISSION_ID_FILE = "submissionId"
    TABLES_CLEARED_FILE = "tablesCleared" # Holds a boolean flag in a file so it can be checked before doing table setup
    tablesCleared = False # Set to true after first time setup is run
    submissionId = None
    passed = False
    finalizePassed = True
    IS_LOCAL = True
    def __init__(self,methodName,interfaces):
        """ Run scripts to clear the job tables and populate with a defined test set """
        super(FileTests,self).__init__(methodName=methodName)
        self.methodName = methodName
        jobTracker = interfaces.jobDb
        self.jobTracker = jobTracker
        self.errorDatabase = interfaces.errorDb
        self.interfaces = interfaces
        try:
            self.tablesCleared = self.toBool(open(self.TABLES_CLEARED_FILE,"r").read())
        except Exception as e:
            # Could not read from file or cast as boolean
            print(str(e))
            self.tablesCleared = False

        if(not self.tablesCleared):
            # Clear job tracker
            #clearJobs()
            self.tablesCleared = True
            open(self.TABLES_CLEARED_FILE,"w").write(str(True))
            # Create submission ID

            self.submissionId = self.insertSubmission(self.jobTracker)

            # Create jobs
            jobValues = {}
            jobValues["uploadFinished"] = [1,4,1]
            jobValues["recordRunning"] = [1,3,2]
            jobValues["externalWaiting"] = [1,1,5]
            jobValues["awardFin"] = [2,2,2]
            jobValues["appropriations"] = [3,2,2]
            jobValues["program_activity"] = [4,2,2]
            self.jobIdDict = {}

            for jobKey, values in jobValues.items():
                job_id = self.insertJob(
                    self.jobTracker,
                    filetype = values[0],
                    status = values[1],
                    type_id = values[2],
                    submission = self.submissionId
                )
                self.jobIdDict[jobKey] = job_id

            # Save jobIdDict to file
            open(self.JOB_ID_FILE,"w").write(json.dumps(self.jobIdDict))
            open(self.SUBMISSION_ID_FILE,"w").write(str(self.submissionId))

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
        fileJson = '{"appropriations":"test1.csv","award_financial":"test2.csv","award":"test3.csv","program_activity":"test4.csv"}'
        if(self.fileResponse == None):
            self.utils.login()
            self.fileResponse = self.utils.postRequest("/v1/submit_files/",fileJson)

    def test_file_submission(self):
        open(FileHandler.VALIDATOR_RESPONSE_FILE,"w").write(str(-1))
        self.call_file_submission()
        self.response = self.fileResponse # Used for displaying failures
        # Test that status is 200
        assert(self.fileResponse.status_code==200)
        # Test Content-Type header
        assert("Content-Type" in self.fileResponse.headers)
        assert(self.fileResponse.headers["Content-Type"]=="application/json")
        # Test message parts for urls

        if(not self.IS_LOCAL):
            assert("_test1.csv" in self.fileResponse.json()["appropriations_key"] )
            assert("_test2.csv" in self.fileResponse.json()["award_financial_key"])
            assert("_test3.csv" in self.fileResponse.json()["award_key"])
            assert("_test4.csv" in self.fileResponse.json()["program_activity_key"])
        else:
            assert("test1.csv" in self.fileResponse.json()["appropriations_key"] )
            assert("test2.csv" in self.fileResponse.json()["award_financial_key"])
            assert("test3.csv" in self.fileResponse.json()["award_key"])
            assert("test4.csv" in self.fileResponse.json()["program_activity_key"])

        for requiredField in ["AccessKeyId","SecretAccessKey","SessionToken","SessionToken"] :
            assert(len(self.fileResponse.json()["credentials"][requiredField]) > 0)

        assert(len(self.fileResponse.json()["bucket_name"]) > 0)

        if(not self.IS_LOCAL):
            self.uploadFileByURL("/"+self.fileResponse.json()["appropriations_key"],"test1.csv")

        # Test that job ids are returned
        responseDict = self.fileResponse.json()
        idKeys = ["program_activity_id", "award_id", "award_financial_id", "appropriations_id"]
        for key in idKeys:
            assert(key in responseDict)
            try:
                int(responseDict[key])
            except:
                self.fail("One of the job ids returned was not an integer")

        # Test that correct user ID is on submission
        submissionId = responseDict["submission_id"]
        user = self.interfaces.userDb.getUserByEmail("user3")
        userId = user.user_id
        submission = self.interfaces.jobDb.getSubmissionById(submissionId)
        assert(submission.user_id == userId) # Check that submission got mapped to the correct user

        # Call upload complete route for each id
        self.check_upload_complete(responseDict["appropriations_id"])
        self.passed = True

    @staticmethod
    def waitOnJob(jobTracker, jobId, status):
        currentID = jobTracker.getStatusId("running")
        targetStatus = jobTracker.getStatusId(status)
        while jobTracker.getStatus(jobId) == currentID:
            time.sleep(1)
        assert(targetStatus == jobTracker.getStatus(jobId))

    def test_check_status(self):
        """ Check that test status route returns correct JSON"""
        utils = TestUtils()
        utils.login()
        self.response = utils.postRequest("/v1/check_status/",'{"submission_id":'+str(self.submissionId)+'}')

        assert(self.response.status_code == 200)
        assert(self.response.json()[str(self.jobIdDict["uploadFinished"])]["status"]=="finished")
        assert(self.response.json()[str(self.jobIdDict["uploadFinished"])]["job_type"]=="file_upload")
        assert(self.response.json()[str(self.jobIdDict["uploadFinished"])]["file_type"]=="award")
        assert(self.response.json()[str(self.jobIdDict["recordRunning"])]["status"]=="running")
        assert(self.response.json()[str(self.jobIdDict["recordRunning"])]["job_type"]=="csv_record_validation")
        assert(self.response.json()[str(self.jobIdDict["recordRunning"])]["file_type"]=="award")
        assert(self.response.json()[str(self.jobIdDict["externalWaiting"])]["status"]=="waiting")
        assert(self.response.json()[str(self.jobIdDict["externalWaiting"])]["job_type"]=="external_validation")
        assert(self.response.json()[str(self.jobIdDict["externalWaiting"])]["file_type"]=="award")
        assert(self.response.json()[str(self.jobIdDict["appropriations"])]["status"]=="ready")
        assert(self.response.json()[str(self.jobIdDict["appropriations"])]["job_type"]=="csv_record_validation")
        assert(self.response.json()[str(self.jobIdDict["appropriations"])]["file_type"]=="appropriations")
        self.passed = True

    def check_error_route(self,jobId,submissonId) :
        jobJson = json.dumps({"upload_id":jobId})
        urlData = self.utils.postRequest("/v1/job_error_report/",jobJson)
        assert("submission_"+str(submissonId)+"_program_activity_error_report" in urlData.json()["error_url"] )
        assert("?Signature" in urlData.json()["error_url"] )
        assert("&AWSAccessKeyId" in urlData.json()["error_url"])

    def check_upload_complete(self, jobId):
        jobJson = json.dumps({"upload_id":jobId})
        self.utils.login()
        self.finalizePassed = False
        self.finalizeResponse = self.utils.postRequest("/v1/finalize_job/",jobJson)
        assert(self.finalizeResponse.status_code == 200)
        self.finalizePassed = True

    @staticmethod
    def uploadFileByURL(s3FileName,filename):
        """ Upload file to S3 and return S3 filename"""
        # Get bucket name
        bucketName = s3UrlHandler.getValueFromConfig("bucket")

        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        fullPath = path + "/" + filename

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
            self.passed = True
            return
        utils = TestUtils()
        utils.login()

        self.setupJobsForReports()
        self.response = utils.postRequest("/v1/submission_error_reports/",'{"submission_id":'+str(self.ERROR_REPORT_SUBMISSION_ID)+'}')
        #clearJobs()  # Clear job DB again so sequence errors don't occur
        assert(self.response.status_code == 200)
        assert(len(self.response.json()) == 4)
        self.passed = True

    @staticmethod
    def check_metrics(submissionId,exists,type_file) :
        utils = TestUtils()
        utils.login()
        response = utils.postRequest("/v1/error_metrics/",'{"submission_id": '+str(submissionId)+ '}')
        assert(response.status_code == 200)
        if(exists) :
            assert(len(response.json()[type_file]) > 0)
        else :
            assert(len(response.json()[type_file]) == 0)


    def test_metrics(self):
        #setup the database for the route test
        submissionId = str(self.insertSubmission(self.jobTracker))

        job = self.insertJob(
            self.jobTracker,
            filetype = "1",
            status = "2",
            type_id = "2",
            submission = submissionId
        )
        self.insertFileStatus(self.errorDatabase,str(job),"1") # Everything Is Fine

        job = self.insertJob(
            self.jobTracker,
            filetype = "2",
            status = "2",
            type_id = "2",
            submission = submissionId
        )
        self.insertFileStatus(self.errorDatabase,str(job),"3") #Bad Header

        job = self.insertJob(
            self.jobTracker,
            filetype = "3",
            status = "2",
            type_id = "2",
            submission = submissionId
        )
        self.insertFileStatus(self.errorDatabase,str(job),"1") # Validation level Errors
        self.insertRowLevelError(self.errorDatabase,str(job))

        #Check the route
        self.check_metrics(submissionId,False,"award")
        self.check_metrics(submissionId,True,"award_financial")
        self.check_metrics(submissionId,True,"appropriations")
        self.passed = True

    @staticmethod

    def insertSubmission(jobTracker, submission = None):
        """ Insert one submission into job tracker and get submission ID back """
        if submission:
            sub = Submission(submission_id = submission, datetime_utc = 0, user_id = 1)
        else:
            sub = Submission(datetime_utc = 0, user_id = 1)
        jobTracker.session.add(sub)
        jobTracker.session.commit()
        return sub.submission_id

    @staticmethod
    def insertJob(jobTracker,filetype,status,type_id,submission, job_id = None):
        """ Insert one job into job tracker and get ID back """
        job = JobStatus(
            file_type_id = filetype,
            status_id = status,
            type_id = type_id,
            submission_id = submission
        )
        if job_id:
            job.job_id = job_id
        jobTracker.session.add(job)
        jobTracker.session.commit()
        return job.job_id

    @staticmethod
    def insertFileStatus(errorDB,job,status):
        """ Insert one file status into error database and get ID back """
        fs = FileStatus(
            job_id = job,
            filename = ' ',
            status_id = status
        )
        errorDB.session.add(fs)
        errorDB.session.commit()
        return fs.file_id

    @staticmethod
    def insertRowLevelError(errorDB,job):
        """ Insert one error into error database """
        ed = ErrorData(
            job_id = job,
            filename = 'test.csv',
            field_name = 'header 1',
            error_type_id = 1,
            occurrences = 100,
            first_row = 123,
            rule_failed = 'Type Check'
        )
        errorDB.session.add(ed)
        errorDB.session.commit()
        return ed.error_data_id

    def setupJobsForReports(self):
        """ Setting Jobs table to correct state for checking error reports from validator unit tests """
        #clearJobs()
        jobTracker = self.jobTracker
        self.tablesCleared = False
        self.insertSubmission(jobTracker, 11)
        self.insertJob(jobTracker,job_id = 11, filetype = 1,
            status = 4, type_id = 2, submission = 11)
        self.insertJob(jobTracker, job_id = 12, filetype = 2,
            status = 4, type_id = 2, submission = 11)
        self.insertJob(jobTracker, job_id = 13, filetype = 3,
            status = 4, type_id = 2, submission = 11)
        self.insertJob(jobTracker, job_id = 15, filetype = 4,
            status = 4, type_id = 2, submission = 11)

    def tearDown(self):
        if(not self.passed):
            print("".join(["Test failed: ",self.methodName]))
            print("Status is " + str(self.response.status_code))
            print(str(self.response.json()))
        if(not self.finalizePassed):
            print("".join(["Test failed: ",self.methodName]))
            print("Status is " + str(self.finalizeResponse.status_code))
            print(str(self.finalizeResponse.json()))

if __name__ == '__main__':
    unittest.main()
