import datetime
import re


class StringCleaner:
    """ Provides basic functionality for sanitizing string inputs """

    @staticmethod
    def clean_string(data, remove_extras=True):
        """ Change to lowercase, trim whitespace on ends, and replace internal spaces with underscores if desired

        Args:
            data: String to be cleaned
            remove_extras: True if spaces and "/" should be replaced with underscores

        Returns:
            Cleaned version of string
        """
        result = str(data).lower().strip()
        if remove_extras:
            # Replace spaces and problematic characters with underscores
            result = result.replace(" ", "_")
            result = result.replace("/", "_")
            result = result.replace("-", "_")
            result = result.replace(",", "_")
            result = result.replace("&", "_")

            # Replace characters that never need to be spaces with nothing
            result = result.replace(".", "")
            result = result.replace("'", "")

            # Remove duplicate underscores by replacing any set of underscores (1 or more) with a single underscore
            result = re.sub("_+", "_", result)
        return result

    @staticmethod
    def split_csv(string):
        """ Split string into a list, excluding empty strings

            Args:
                string: the string to split

            Returns:
                Empty array if the string is empty or an array of whitespace-trimmed strings split on "," from the
                original
        """
        if string is None:
            return []
        return [n.strip() for n in string.split(',') if n]

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
            datetime.datetime.strptime(data, '%m/%d/%Y')
            return True
        except ValueError:
            return False
