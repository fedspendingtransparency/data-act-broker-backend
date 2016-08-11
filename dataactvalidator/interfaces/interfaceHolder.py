from dataactcore.models.baseInterface import BaseInterface
from dataactvalidator.interfaces.validatorErrorInterface import ValidatorErrorInterface
from dataactvalidator.interfaces.validatorJobTrackerInterface import ValidatorJobTrackerInterface
from dataactvalidator.interfaces.validatorStagingInterface import ValidatorStagingInterface
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface

class InterfaceHolder:
    """ This class holds an interface to each database, to allow reuse of connections throughout one thread """

    def __init__(self):
        """ Create the interfaces """
        if BaseInterface.interfaces is None:
            self.jobDb = ValidatorJobTrackerInterface()
            self.errorDb = ValidatorErrorInterface()
            self.stagingDb = ValidatorStagingInterface()
            self.validationDb = ValidatorValidationInterface()
            BaseInterface.interfaces = self
        else:
            self.jobDb = BaseInterface.interfaces.jobDb
            self.errorDb = BaseInterface.interfaces.errorDb
            self.stagingDb = BaseInterface.interfaces.stagingDb
            self.validationDb = BaseInterface.interfaces.validationDb

    def close(self):
        """ Close all open connections """
        InterfaceHolder.closeOne(self.jobDb)
        InterfaceHolder.closeOne(self.errorDb)
        InterfaceHolder.closeOne(self.stagingDb)
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
            except:
                pass