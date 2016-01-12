import json
import unittest
from handlers.managerProxy import ManagerProxy
from test.baseTest import BaseTest
from test.testUtils import TestUtils
from dataactcore.scripts.createJobTables import createJobTables
from dataactcore.scripts.clearJobs import clearJobs
from handlers.interfaceHolder import InterfaceHolder

class FileTests(BaseTest):
    """ Test file submission routes """
    fileResponse = None
    CHECK_VALIDATOR = False
    tablesCleared = False # Set to true after first time setup is run

    def __init__(self,methodName):
        """ Run scripts to clear the job tables and populate with a defined test set """
        super(FileTests,self).__init__(methodName=methodName)
        jobTracker = InterfaceHolder.JOB_TRACKER

        if(not self.tablesCleared):
            # Clear job tracker
            createJobTables()
            self.tablesCleared = True

            if(len(jobTracker.getJobsBySubmission(1))==0):
                sqlStatements = [
                    "INSERT INTO submission (datetime_utc) VALUES (0)",
                    "INSERT INTO job_status (file_type_id, status_id, type_id, submission_id) VALUES (1,4,1,1),(1,3,2,1),(1,1,5,1),(2,2,2,1),(3,2,2,1),(4,2,2,1)"
                ]

                for statement in sqlStatements:
                    jobTracker.runStatement(statement)

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
        response = utils.postRequest("/v1/check_status/",'{"submission_id":1}')

        assert(response.status_code == 200)
        assert(response.json()["1"]["status"]=="finished")
        assert(response.json()["1"]["job_type"]=="file_upload")
        assert(response.json()["1"]["file_type"]=="award")
        assert(response.json()["2"]["status"]=="running")
        assert(response.json()["2"]["job_type"]=="csv_record_validation")
        assert(response.json()["2"]["file_type"]=="award")
        assert(response.json()["3"]["status"]=="waiting")
        assert(response.json()["3"]["job_type"]=="external_validation")
        assert(response.json()["3"]["file_type"]=="award")
        assert(response.json()["5"]["status"]=="ready")
        assert(response.json()["5"]["job_type"]=="csv_record_validation")
        assert(response.json()["5"]["file_type"]=="appropriations")

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
        response = proxy.sendJobRequest(jobId)
        assert(response.status_code == 200)

    def test_error_report(self):
        # Will only pass if validator unit tests have been run to generate the error reports
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
