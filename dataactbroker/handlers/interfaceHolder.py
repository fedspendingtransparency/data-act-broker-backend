import sys
import traceback
from dataactcore.utils.cloudLogger import CloudLogger
from dataactcore.models.baseInterface import BaseInterface
from dataactbroker.handlers.errorHandler import ErrorHandler
from dataactbroker.handlers.jobHandler import JobHandler
from dataactbroker.handlers.userHandler import UserHandler
from dataactbroker.interfaces.validationBrokerInterface import ValidationBrokerInterface

class InterfaceHolder:
    """ This class holds an interface to each database as a static variable, to allow reuse of connections throughout the project """
    def __init__(self):
        """ Create the interfaces """
        if BaseInterface.interfaces is None:
            self.jobDb = JobHandler()
            self.errorDb = ErrorHandler()
            self.userDb = UserHandler()
            self.validationDb = ValidationBrokerInterface()
            BaseInterface.interfaces = self
        else:
            self.jobDb = BaseInterface.interfaces.jobDb
            self.errorDb = BaseInterface.interfaces.errorDb
            self.userDb = BaseInterface.interfaces.userDb
            self.validationDb = BaseInterface.interfaces.validationDb

    def close(self):
        """ Close all open connections """
        InterfaceHolder.closeOne(self.jobDb)
        InterfaceHolder.closeOne(self.errorDb)
        InterfaceHolder.closeOne(self.userDb)
        InterfaceHolder.closeOne(self.validationDb)

    @staticmethod
    def closeOne(interface):
        """ Close all aspects of one interface """
        if(interface == None):
            # No need to close a nonexistent connection
            return
        try:
            if(interface.session == None):
                # If session is None, skip closing
                return
        except AttributeError as e:
            # If interface has no session, skip closing
            return

        # Try to close the session and connection, on error try a rollback
        try:
            interface.session.close()
        except:
            try:
                interface.session.rollback()
                interface.session.close()
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                trace = traceback.extract_tb(exc_tb, 10)
                CloudLogger.logError('Broker DB Interface Error: ', e, trace)
                del exc_tb
                raise
