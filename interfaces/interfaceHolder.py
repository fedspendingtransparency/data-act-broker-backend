from interfaces.errorInterface import ErrorInterface
from interfaces.jobTrackerInterface import JobTrackerInterface
from interfaces.stagingInterface import StagingInterface
from interfaces.validationInterface import ValidationInterface

class InterfaceHolder:
    JOB_TRACKER = JobTrackerInterface()
    ERROR = ErrorInterface()
    STAGING = StagingInterface()
    VALIDATION = ValidationInterface()