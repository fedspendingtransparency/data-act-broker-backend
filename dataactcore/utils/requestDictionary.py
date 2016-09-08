import json
from dataactcore.utils.responseException import ResponseException
from werkzeug.exceptions import BadRequest

class RequestDictionary() :
    """ Provides an interface to an http request """

    def __init__(self,request,optionalRequest=False):
        try:
            self.requestDict = None
            if(not ("Content-Type" in request.headers)):
                raise ValueError("Must include Content-Type header")

            # Allowing extra content after application/json for firefox compatibility
            if("application/json" in request.headers["Content-Type"]):
                    self.requestDict = request.get_json()
            elif(request.headers["Content-Type"] == "application/x-www-form-urlencoded"):
                    self.requestDict = request.form
            else :
                raise ValueError("Invaild Content-Type : "+  request.headers["Content-Type"] )
            if(not(isinstance(self.requestDict,dict))):
                raise TypeError("Failed to create a dictionary out of json")
        except BadRequest as br:
            if optionalRequest:
                self.requestDict = {}
            else:
                raise br


    def getValue(self,value) :
        """ Returns value for specified key """
        if(not(value in self.requestDict)):
            raise ValueError(value + " not found")
        return self.requestDict[value]
        
    def exists(self,value) :
        """ Returns True if key is in request json """
        if(not(value in self.requestDict)):
            return False
        return True

    def to_string(self):
        return str(self.requestDict)
