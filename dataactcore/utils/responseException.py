class ResponseException(Exception):
    def __init__(self,message):
        super(ResponseException,self).__init__(message)
        self.status = 500 # This will be used for the HTTP response status code, 500 if unspecified
        self.errorReportMessage = None
        self.wrappedException = None # Can be used to wrap another type of exception into a ResponseException