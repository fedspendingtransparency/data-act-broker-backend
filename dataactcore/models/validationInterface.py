from dataactcore.models.baseInterface import BaseInterface

class ValidationInterface(BaseInterface):
    """Manages all interaction with the validation/staging database."""

    def __init__(self):
        super(ValidationInterface, self).__init__()
