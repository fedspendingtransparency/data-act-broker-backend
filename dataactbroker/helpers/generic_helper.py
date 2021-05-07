import re
import calendar
import logging
import requests
from requests.packages.urllib3.exceptions import ReadTimeoutError
from dateutil.parser import parse
import datetime as dt

from suds.transport.https import HttpAuthenticated as SudsHttpsTransport
from urllib.request import HTTPBasicAuthHandler

from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.sql.sqltypes import String, DateTime, NullType, Date

from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

logger = logging.getLogger(__name__)
logging.getLogger('requests').setLevel(logging.WARNING)

RETRY_REQUEST_EXCEPTIONS = (requests.exceptions.RequestException, ConnectionError, ConnectionResetError,
                            ReadTimeoutError, requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout,
                            requests.exceptions.ChunkedEncodingError)


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


class WellBehavedHttpsTransport(SudsHttpsTransport):
    """ HttpsTransport which properly obeys the ``*_proxy`` environment variables."""

    def u2handlers(self):
        """ Return a list of specific handlers to add.

        The urllib2 logic regarding ``build_opener(*handlers)`` is:

        - It has a list of default handlers to use

        - If a subclass or an instance of one of those default handlers is given
            in ``*handlers``, it overrides the default one.

        Suds uses a custom {'protocol': 'proxy'} mapping in self.proxy, and adds
        a ProxyHandler(self.proxy) to that list of handlers.
        This overrides the default behaviour of urllib2, which would otherwise
        use the system configuration (environment variables on Linux, System
        Configuration on Mac OS, ...) to determine which proxies to use for
        the current protocol, and when not to use a proxy (no_proxy).

        Thus, passing an empty list (asides from the BasicAuthHandler)
        will use the default ProxyHandler which behaves correctly.
        """
        return [HTTPBasicAuthHandler(self.pm)]


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
        except Exception:
            raise TypeError('{} needs to be a valid date/datetime string'.format(raw_date))
    elif not isinstance(raw_date, (dt.date, dt.datetime)):
        raise TypeError('{} needs to be a valid date/datetime'.format(raw_date))

    result = raw_date.year
    if raw_date.month > 9:
        result += 1

    return result


def batch(iterable, n=1):
    """ Simple function to create batches from a list

        Args:
            iterable: the list to be batched
            n: the size of the batches

        Yields:
            the same list (iterable) in batches depending on the size of N
    """
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx:min(ndx + n, length)]
