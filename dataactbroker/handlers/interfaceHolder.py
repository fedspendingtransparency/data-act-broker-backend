from dataactbroker.handlers.errorHandler import ErrorHandler
from dataactbroker.handlers.jobHandler import JobHandler
from dataactbroker.handlers.userHandler import UserHandler

class InterfaceHolder:
    """ This class holds an interface to each database as a static variable, to allow reuse of connections throughout the project """
    def __init__(self):
        """ Create the interfaces """
        self.jobDb = JobHandler()
        self.errorDb = ErrorHandler()
        self.userDb = UserHandler()

    def close(self):
        """ Close all open connections """
        InterfaceHolder.closeOne(self.jobDb)
        InterfaceHolder.closeOne(self.errorDb)
        InterfaceHolder.closeOne(self.userDb)

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
            except Exception as e:
                interface.logDbError(e)