import json
class RequestDictionary() :
    def __init__(self,request):

        self.requestDict = None
        if(not ("Content-Type" in request.headers)):
            raise ValueError("Must include Content-Type header")

        if(request.headers["Content-Type"] == "application/json"):
                self.requestDict = request.get_json()
        elif(request.headers["Content-Type"] == "application/x-www-form-urlencoded"):
                self.requestDict = request.form
        else :
            raise ValueError("Invaild Content-Type : "+  request.headers["Content-Type"] )
        if(not(isinstance(self.requestDict,dict))):
            raise TypeError("Failed to create a dictionary out of json")


    def getValue(self,value) :
        if(not(value in self.requestDict)):
            raise ValueError(value + " not found")
        return self.requestDict[value]
