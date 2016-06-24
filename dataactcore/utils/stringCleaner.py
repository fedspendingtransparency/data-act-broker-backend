class StringCleaner:
    """ Provides basic functionality for sanitizing string inputs """

    @staticmethod
    def cleanString(data,removeSpaces = True):
        """ Change to lowercase, trim whitespace on ends, and replace internal spaces with underscores if desired

        Args:
            data: String to be cleaned
            removeSpaces: True if spaces should be replaced with underscores

        Returns:
            Cleaned version of string
        """
        result = str(data).lower().strip()
        if(removeSpaces):
            result = result.replace(" ","_")
        return result

    @staticmethod
    def isNumeric(data):
        try:
            float(data)
            return True
        except:
            return False