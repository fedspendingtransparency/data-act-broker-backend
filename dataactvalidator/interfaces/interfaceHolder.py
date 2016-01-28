from dataactvalidator.interfaces.errorInterface import ErrorInterface
from dataactvalidator.interfaces.jobTrackerInterface import JobTrackerInterface
from dataactvalidator.interfaces.stagingInterface import StagingInterface
from dataactvalidator.interfaces.validationInterface import ValidationInterface

class InterfaceHolder:
    JOB_TRACKER = JobTrackerInterface()
    ERROR = ErrorInterface()
    STAGING = StagingInterface()
    VALIDATION = ValidationInterface()
