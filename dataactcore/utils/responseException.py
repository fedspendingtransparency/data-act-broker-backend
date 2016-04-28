class ResponseException(Exception):
    """ Exception wrapper to be used in an http response, allows exceptions to specify status codes when raised """
    def __init__(self,message,status = 500,errorClass = None,errorType = None, **kwargs):
        super(ResponseException,self).__init__(message)
        self.status = status # This will be used for the HTTP response status code, 500 if unspecified
        self.errorType = errorType # This is used for writing to the error DB
        self.extraInfo = {}
        for key in kwargs:
            # Include extra error info
            self.extraInfo[key] = kwargs[key] # This can be written to the error DB for some error types
        if(errorClass == None):
            self.wrappedException = None # Can be used to wrap another type of exception into a ResponseException
        else:
            try:
                self.wrappedException = errorClass(message)
            except Exception as e:
                # Some errors cannot be created with just a message, in that case create a string representation
                self.wrappedException = str(errorClass) + str(message)