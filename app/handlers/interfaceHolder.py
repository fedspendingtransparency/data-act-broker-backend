from handlers.errorHandler import ErrorHandler
from handlers.jobHandler import JobHandler
from handlers.userHandler import UserHandler

class InterfaceHolder:
    JOB_TRACKER = JobHandler()
    ERROR = ErrorHandler()
    USER = UserHandler()