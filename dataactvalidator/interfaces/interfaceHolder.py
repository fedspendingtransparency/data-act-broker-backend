from dataactvalidator.interfaces.errorInterface import ErrorInterface
from dataactvalidator.interfaces.jobTrackerInterface import JobTrackerInterface
from dataactvalidator.interfaces.stagingInterface import StagingInterface
from dataactvalidator.interfaces.validationInterface import ValidationInterface

class InterfaceHolder:
    """ This class holds an interface to each database as a static variable, to allow reuse of connections throughout the project """
    JOB_TRACKER = JobTrackerInterface()
    ERROR = ErrorInterface()
    STAGING = StagingInterface()
    VALIDATION = ValidationInterface()
