import json
from aws.s3UrlHandler import s3UrlHandler
from utils.requestDictionary import RequestDictionary
class FileHandler:

    def __init__(self,request,response):
        self.request = request
        self.response = response
        self.s3manager = s3UrlHandler()
    def submit(self):
        responseDict = {"message":"File URLs attached"}
        self.response.headers["Content-Type"] = "application/json"
        try:

            self.response.status_code = 200
            #TODO implement
            safeDictionary = RequestHandler(request)

            # Generate URLs for each file requested
            raise NotImplementedError("S3 not available yet")
            responseDict["appropriations_url"] = self.s3manager.getSignedUrl()
            responseDict["award_financial_url"] = self.s3manager.getSignedUrl()
            responseDict["award_url"] = self.s3manager.getSignedUrl()
            responseDict["procurement_url"] = self.s3manager.getSignedUrl()
            self.response.set_data(json.dumps(responseDict))
        except (ValueError , TypeError, NotImplementedError) as e:
            self.response.status_code = 400
            responseDict["message"] = e.message
            responseDict["errorType"] = str(type(e))
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
