class ResponseException(Exception):
    """ Exception wrapper to be used in an http response, allows exceptions to specify status codes when raised """
    def __init__(self,message,status = 500,errorClass = None,errorType = None):
        super(ResponseException,self).__init__(message)
        self.status = status # This will be used for the HTTP response status code, 500 if unspecified
        self.errorType = errorType # This is used for writing to the error DB
        if(errorClass == None):
            self.wrappedException = None # Can be used to wrap another type of exception into a ResponseException
        else:
            self.wrappedException = errorClass(message)