from dateutil.parser import parse


class StringCleaner:
    """ Provides basic functionality for sanitizing string inputs """

    @staticmethod
    def clean_string(data, remove_spaces=True):
        """ Change to lowercase, trim whitespace on ends, and replace internal spaces with underscores if desired

        Args:
            data: String to be cleaned
            remove_spaces: True if spaces should be replaced with underscores

        Returns:
            Cleaned version of string
        """
        result = str(data).lower().strip()
        if remove_spaces:
            result = result.replace(" ", "_")
        return result

    @staticmethod
    def is_numeric(data):
        try:
            float(data)
            return True
        except ValueError:
            return False

    @staticmethod
    def is_date(data):
        try:
            parse(data)
            return True
        except (ValueError, OverflowError):
            return False
