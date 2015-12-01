import json
from aws.s3UrlHandler import s3UrlHandler

class FileHandler:

    def __init__(self,request,response):
        self.request = request
        self.response = response
        self.s3manager = s3UrlHandler()

    def submit(self):
        responseDict = {"message":"File URLs attached"}
        try:
            self.response.headers["Content-Type"] = "application/json"
            self.response.status_code = 200

            if(not ("Content-Type" in self.request.headers)):
                # Content type not defined
                raise ValueError("Must include Content-Type header")

            if(self.request.headers["Content-Type"] == "application/json"):
                requestDict = self.request.get_json()
            elif(self.request.headers["Content-Type"] == "application/x-www-form-urlencoded"):
                requestDict = self.request.form
            # Generate URLs for each file requested
            raise NotImplementedError("S3 not available yet")
            responseDict["appropriations_url"] = self.s3manager.getSignedUrl()
            responseDict["award_financial_url"] = self.s3manager.getSignedUrl()
            responseDict["award_url"] = self.s3manager.getSignedUrl()
            responseDict["procurement_url"] = self.s3manager.getSignedUrl()
            self.response.set_data(json.dumps(responseDict))
        except ValueError as e:
            self.response.status_code = 400
            responseDict["message"] = e.message
            responseDict["errorType"] = str(type(e))
            self.response.set_data(json.dumps(responseDict))
            return self.response