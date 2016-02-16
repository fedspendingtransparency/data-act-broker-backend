from dataactvalidator.interfaces.errorInterface import ErrorInterface
from dataactvalidator.interfaces.jobTrackerInterface import JobTrackerInterface
from dataactvalidator.interfaces.stagingInterface import StagingInterface
from dataactvalidator.interfaces.validationInterface import ValidationInterface

class InterfaceHolder:
    """ This class holds an interface to each database as a static variable, to allow reuse of connections throughout the project """
    JOB_TRACKER = None
    ERROR = None
    STAGING = None
    VALIDATION = None

    @staticmethod
    def connect():
        """ Create the interfaces """
        InterfaceHolder.JOB_TRACKER = JobTrackerInterface()
        InterfaceHolder.ERROR = ErrorInterface()
        InterfaceHolder.STAGING = StagingInterface()
        InterfaceHolder.VALIDATION = ValidationInterface()

    @staticmethod
    def close():
        """ Close all open connections """
        InterfaceHolder.closeOne(InterfaceHolder.JOB_TRACKER)
        InterfaceHolder.closeOne(InterfaceHolder.ERROR)
        InterfaceHolder.closeOne(InterfaceHolder.STAGING)
        InterfaceHolder.closeOne(InterfaceHolder.VALIDATION)

    @staticmethod
    def closeOne(interface):
        """ Close all aspects of one interface """
        if(interface == None):
            # No need to close a nonexistent connection
            return

        # Try to close the session and connection, on error try a rollback
        try:
            interface.session.close()
        except:
            try:
                interface.session.rollback()
                interface.session.close()
            except:
                pass