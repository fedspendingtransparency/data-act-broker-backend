import re
import calendar

from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode


def quarter_to_dates(quarter):
    """ Converts a quarter of format Q#/YYYY to the real-life start and end dates it represents.

        Args:
            quarter: string representing the quarter/year to convert

        Returns:
            Strings representing the start and end dates of the given quarter
    """
    # Make sure quarter is in the proper format
    if not quarter or not re.match('Q[1-4]/\d{4}', quarter):
        raise ResponseException('Quarter must be in Q#/YYYY format, where # is 1-4.', StatusCode.CLIENT_ERROR)

    split_quarter = quarter.split('/')

    # set the year to the one given if anything other than Q1, if Q1 set it to the previous year
    year = int(split_quarter[1]) if split_quarter[0] != 'Q1' else int(split_quarter[1]) - 1

    # Get the last month of the quarter (set to December if it's 0)
    last_month = (int(split_quarter[0][1]) - 1) * 3
    if last_month == 0:
        last_month = 12
    # Get the last day of the last month
    last_day_of_month = calendar.monthrange(year, last_month)[1]

    first_month = last_month - 2
    start = str(first_month).zfill(2) + '/01/' + str(year)
    end = str(last_month).zfill(2) + '/' + str(last_day_of_month) + '/' + str(year)

    return start, end
