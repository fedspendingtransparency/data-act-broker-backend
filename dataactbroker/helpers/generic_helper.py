import re
import calendar
from dateutil.parser import parse
import datetime as dt

from suds.client import Client

from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.sql.sqltypes import String, DateTime, NullType, Date

from dataactcore.config import CONFIG_BROKER
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode


class StringLiteral(String):
    """ Teach SA how to literalize various things """

    def __init__(self, *args, **kwargs):
        self._enums = kwargs.pop('_enums', None)
        super(StringLiteral, self).__init__(*args, **kwargs)

    def literal_processor(self, dialect):
        """ Overwritten method to populate variables in SQL """

        super_processor = super(StringLiteral, self).literal_processor(dialect)

        def process(value):
            """ Overwritten method to populate variables in SQL """

            if isinstance(value, int):
                return str(value)
            if not isinstance(value, str):
                value = str(value)
            result = super_processor(value)
            if isinstance(result, bytes):
                result = result.decode(dialect.encoding)
            return result
        return process


class LiteralDialect(PGDialect):
    """ Special type to populate variables in SQL """

    colspecs = {
        # prevent various encoding explosions
        String: StringLiteral,
        # teach SA about how to literalize a datetime
        DateTime: StringLiteral,
        # teach SA about how to literalize a datetime
        Date: StringLiteral,
        # don't format py2 long integers to NULL
        NullType: StringLiteral,
    }


def year_period_to_dates(year, period):
    """ Converts a year and period to the real-life start and end dates they represents.

        Args:
            year: integer representing the year to use
            period: integer representing the period (month of the fiscal year) to use

        Returns:
            Strings representing the start and end dates of the given quarter
    """
    # Make sure year is in the proper format
    if not year or not re.match('^\d{4}$', str(year)):
        raise ResponseException('Year must be in YYYY format.', StatusCode.CLIENT_ERROR)
    # Make sure period is a number 2-12
    if not period or period not in list(range(2, 13)):
        raise ResponseException('Period must be an integer 2-12.', StatusCode.CLIENT_ERROR)

    # Set the actual month, add 12 if it's negative so it loops around and adjusts the year
    month = period - 3
    if month < 1:
        month += 12
        year -= 1

    # Get the last day of the month
    last_day_of_month = calendar.monthrange(year, month)[1]

    start = str(month).zfill(2) + '/01/' + str(year)
    end = str(month).zfill(2) + '/' + str(last_day_of_month) + '/' + str(year)

    return start, end


def format_internal_tas(row):
    """ Concatenate TAS components into a single field for internal use.

        Args:
            row: row of data with TAS elements

        Returns:
            TAS components concatenated into a single string
    """
    # This formatting should match formatting in dataactcore.models.stagingModels concat_tas
    ata = row['allocation_transfer_agency'].strip() if row['allocation_transfer_agency'] and \
        row['allocation_transfer_agency'].strip() else '000'
    aid = row['agency_identifier'].strip() if row['agency_identifier'] and row['agency_identifier'].strip() else '000'
    bpoa = row['beginning_period_of_availa'].strip() if row['beginning_period_of_availa'] and \
        row['beginning_period_of_availa'].strip() else '0000'
    epoa = row['ending_period_of_availabil'].strip() if row['ending_period_of_availabil'] and \
        row['ending_period_of_availabil'].strip() else '0000'
    atc = row['availability_type_code'].strip() if row['availability_type_code'] and \
        row['availability_type_code'].strip() else ' '
    mac = row['main_account_code'].strip() if row['main_account_code'] and row['main_account_code'].strip() else '0000'
    sac = row['sub_account_code'].strip() if row['sub_account_code'] and row['sub_account_code'].strip() else '000'
    return ''.join([ata, aid, bpoa, epoa, atc, mac, sac])


def get_client():
    """ Get the Client to access SAM. """
    try:
        client = Client(CONFIG_BROKER['sam']['wsdl'])
    except ConnectionResetError:
        raise ResponseException("Unable to contact SAM service, which may be experiencing downtime or intermittent "
                                "performance issues. Please try again later.", StatusCode.NOT_FOUND)
    return client


def generate_raw_quoted_query(queryset):
    """ Generates the raw sql from a queryset

        Args:
            queryset: SQLAlchemy object to parse

        Returns:
            raw SQL string equivalent to the queryset
    """
    return str(queryset.statement.compile(dialect=LiteralDialect(), compile_kwargs={"literal_binds": True}))\
        .replace('\n', ' ')


def fy(raw_date):
    """ Get fiscal year from date, datetime, or date string

        Args:
            raw_date: date to be parsed

        Returns:
            integer representing the fiscal year associated with the date
    """

    if raw_date is None:
        return None

    if isinstance(raw_date, str):
        try:
            raw_date = parse(raw_date)
        except:
            raise TypeError('{} needs to be a valid date/datetime string'.format(raw_date))
    elif not isinstance(raw_date, (dt.date, dt.datetime)):
        raise TypeError('{} needs to be a valid date/datetime'.format(raw_date))

    result = raw_date.year
    if raw_date.month > 9:
        result += 1

    return result
