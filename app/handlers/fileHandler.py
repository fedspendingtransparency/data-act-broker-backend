import json
from aws.s3UrlHandler import s3UrlHandler

class FileHandler:

    def __init__(self,request,response):
        self.request = request
        self.response = response
        self.s3manager = s3UrlHandler()

    def submit(self):
        self.response.headers["Content-Type"] = "application/json"
        self.response.status_code = 200
        responseDict = {"message":"File URLs attached"}
        # Generate URLs for each file requested
        responseDict["appropriations_url"] = self.s3manager.getUrl()
        responseDict["award_financial_url"] = self.s3manager.getUrl()
        responseDict["award_url"] = self.s3manager.getUrl()
        responseDict["procurement_url"] = self.s3manager.getUrl()
        self.response.set_data(json.dumps(responseDict))