class ResponseException(Exception):
    def __init__(self,message,status = 500,errorType = None):
        super(ResponseException,self).__init__(message)
        self.status = status # This will be used for the HTTP response status code, 500 if unspecified
        if(errorType == None):
            self.wrappedException = None # Can be used to wrap another type of exception into a ResponseException
        else:
            self.wrappedException = errorType(message)