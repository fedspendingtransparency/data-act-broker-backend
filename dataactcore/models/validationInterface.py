from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.validationModels import FileColumn, FileTypeValidation

class ValidationInterface(BaseInterface):
    """Manages all interaction with the validation/staging database."""

    def __init__(self):
        super(ValidationInterface, self).__init__()

    def getFileTypeList(self):
        """ Return list of file types """
        fileTypes = self.session.query(FileTypeValidation.name).all()
        # Convert result into list
        return [fileType.name for fileType in fileTypes]

    def getLongToShortColname(self):
        """Return a dictionary that maps schema field names to shorter, machine-friendly versions."""
        query = self.session.query(FileColumn.name, FileColumn.name_short).all()
        dict = {row.name: row.name_short for row in query}
        return dict

    def getShortToLongColname(self):
        """Return a dictionary that maps short, machine-friendly schema names to their long versions."""
        query = self.session.query(FileColumn.name, FileColumn.name_short).all()
        dict = {row.name_short: row.name for row in query}
        return dict
