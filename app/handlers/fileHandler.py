import json
import sys
import traceback
from aws.s3UrlHandler import s3UrlHandler
from utils.requestDictionary import RequestDictionary
from jobHandler import JobHandler

class FileHandler:

    FILE_TYPES = ["appropriations","award_financial","award","procurement"]
    def __init__(self,request,response):
        self.request = request
        self.response = response
        self.s3manager = s3UrlHandler("DatactFiles","TestUser")

    # Submit set of files
    def submit(self):
        responseDict = {"message":"File URLs attached"}
        self.response.headers["Content-Type"] = "application/json"
        try:
            # TODO move this code into file handler, based on actual file names
            jobManager = JobHandler()
            jobManager.createJobs(["award.csv","awardFinancial.csv","procurement.csv","appropriation.csv"])
            self.response.status_code = 200
            self.response.set_data(json.dumps({"message":"Job tracker DB working"}))
            # If no exceptions, return response
            return self.response

            safeDictionary = RequestDictionary(self.request)
            for fileName in FileHandler.FILE_TYPES :
                if( safeDictionary.exists(fileName+"_url")) :
                    responseDict[fileName+"_url"] = self.s3manager.getSignedUrl(safeDictionary.getValue(fileName+"_url"))
            self.response.set_data(json.dumps(responseDict))
        except (ValueError , TypeError, NotImplementedError) as e:
            self.response.status_code = 400
            responseDict["message"] = e.message
            responseDict["errorType"] = str(type(e))
            self.response.set_data(json.dumps(responseDict))
            return self.response
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            self.response.status_code = 500
            exc_type, exc_obj, exc_tb = sys.exc_info()
            responseDict["message"] = e.message
            responseDict["errorType"] = str(type(e))
            responseDict["errorArgs"] = e.args
            trace = traceback.extract_tb(exc_tb, 10)
            #print(str(type(e)))
            #print(e.message)
            #print(trace)
            responseDict["trace"] = trace
            del exc_tb
            self.response.set_data(json.dumps(responseDict))
            return self.response

    def complete(self):
        self.response.headers["Content-Type"] = "application/json"
        try:
            safeDictionary = RequestHandler(request)
            fileID = safeDictionary.getValue("upload_id")
            #TODO Update database here
            self.response.status_code = 200
            responseDict["success"] = "True"
            self.response.set_data(json.dumps(responseDict))
        except ( ValueError , TypeError ) as e:
            self.response.status_code = 400
            responseDict["message"] = e.message
            responseDict["errorType"] = str(type(e))
            self.response.set_data(json.dumps(responseDict))
            return self.response
