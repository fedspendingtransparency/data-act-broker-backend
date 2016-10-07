from dataactcore.models.errorInterface import ErrorInterface

class ValidatorErrorInterface(ErrorInterface):
    """ Manages communication with the error database """

    def __init__(self):
        super(ValidatorErrorInterface, self).__init__()


