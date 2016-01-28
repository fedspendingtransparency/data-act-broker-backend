from handlers.errorHandler import ErrorHandler
from handlers.jobHandler import JobHandler
from handlers.userHandler import UserHandler

class InterfaceHolder:
    """ This class holds an interface to each database as a static variable, to allow reuse of connections throughout the project """
    JOB_TRACKER = JobHandler()
    ERROR = ErrorHandler()
    USER = UserHandler()