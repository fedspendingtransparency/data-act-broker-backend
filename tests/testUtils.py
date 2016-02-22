import requests

class TestUtils(object):
    # Test basic routes, including login and file submission
    BASE_URL = "http://127.0.0.1:80"
    #BASE_URL = "http://54.173.199.34:80"

    JSON_HEADER = {"Content-Type": "application/json"}

    def getRequest(self,url) :
        try:
            current = self.cookies
        except AttributeError:
            self.cookies = {}
        responseData = requests.request(method="GET", url=TestUtils.BASE_URL + url, headers = TestUtils.JSON_HEADER,cookies=self.cookies)
        self.cookies =  responseData.cookies
        return responseData

    def postRequest(self,url,jsonData,headers = None, ignoreBase = False, method = "POST") :
        """

        Args:
            url - Route to call
            jsonData - String representation of a JSON

        Returns:
            Response object returned by route
        """
        try:
            current = self.cookies
        except AttributeError:
            self.cookies = {}
        if(headers == None):
            headers = TestUtils.JSON_HEADER
        if not ignoreBase:
            url = TestUtils.BASE_URL + url

        responseData = requests.request(method=method, url=url, data=jsonData, headers = headers,cookies=self.cookies,timeout=500)
        self.cookies =  responseData.cookies
        return responseData

    # Send login route call
    def login(self,username = "user3",password = "123abc"):
        userJson = '{"username":"'+username+'","password":"'+password+'"}'
        return self.postRequest("/v1/login/",userJson)

    # Call logout route
    def logout(self):
        return self.postRequest("/v1/logout/",{})

    def checkResponse(self,response,status,message = None):
        # Check status is 200
        assert(response.status_code == status)
        # Check JSON content type header
        assert("Content-Type" in response.headers), "No content type specified"
        # Test content type is correct
        assert(response.headers["Content-Type"]=="application/json"), "Content type is not json"
        # Make sure json part of response exists and is a dict
        json = response.json()
        assert(str(type(json))=="<type 'dict'>"), "json component is not a dict"
        # Test content of json
        if(message != None):
            assert(json["message"] == message), "Incorrect content in json string"