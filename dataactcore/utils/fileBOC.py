import datetime

from collections import OrderedDict
from sqlalchemy import or_, and_, func, null
from sqlalchemy.orm import outerjoin
from sqlalchemy.sql.expression import case, literal_column

from dataactcore.models.domainModels import SF133, TASLookup, CGAC, FREC, TASFailedEdits
from dataactcore.models.stagingModels import PublishedObjectClassProgramActivity
from dataactcore.models.jobModels import SubmissionWindowSchedule

mapping = OrderedDict([
    ('display_tas', ['TAS']),
    ('allocation_transfer_agency', ['AllocationTransferAgencyIdentifier']),
    ('agency_identifier', ['AgencyIdentifier']),
    ('beginning_period_of_availa', ['BeginningPeriodOfAvailability']),
    ('ending_period_of_availabil', ['EndingPeriodOfAvailability']),
    ('availability_type_code', ['AvailabilityTypeCode']),
    ('main_account_code', ['MainAccountCode']),
    ('sub_account_code', ['SubAccountCode']),
    ('object_class', ['ObjectClass']),
    ('by_direct_reimbursable_fun', ['ByDirectReimbursableFundingSource']),
    ('disaster_emergency_fund_code', ['DisasterEmergencyFundCode']),
    ('pya', ['PriorYearAdjustment']),
    ('begin_end', ['BeginningEndingIndicator']),
    ('reporting_fiscal_year', ['ReportingFiscalYear']),
    ('reporting_fiscal_period', ['ReportingFiscalPeriod']),
    ('ussgl_account_num', ['USSGLAccountNumber']),
    ('dollar_amount_gtas', ['DollarAmountGTAS']),
    ('dollar_amount_broker', ['DollarAmountDataBroker']),
    ('dollar_amount_diff', ['DollarAmountGTASSubDataBroker']),
    ('file_b_rows', ['FileBRows'])
])
db_columns = [key for key in mapping]


def query_data(session, agency_code, period, year):
    """ Request A file data

        Args:
            session: DB session
            agency_code: FREC or CGAC code for generation
            period: The period for which to get GTAS data
            year: The year for which to get GTAS data

        Returns:
            The rows using the provided dates for the given agency.
    """
    rows = initial_query(session, PublishedObjectClassProgramActivity).\
        filter(PublishedObjectClassProgramActivity.submission_id == 1)

    return rows


def initial_query(session, model):
    """ Creates the initial query for D2 files.

        Args:
            session: The current DB session
            model: subquery model to get data from
            year: the year for which to get data

        Returns:
            The base query (a select from the tas/gtas tables with the specified columns).
    """
    return session.query(
        model.display_tas,
        model.allocation_transfer_agency,
        model.agency_identifier,
        model.beginning_period_of_availa,
        model.ending_period_of_availabil,
        model.availability_type_code,
        model.main_account_code,
        model.sub_account_code,
        model.object_class,
        model.submission_id.label('by_direct_reimbursable_fun'),
        model.disaster_emergency_fund_code,
        model.submission_id.label('pya'),
        model.submission_id.label('begin_end'),
        model.submission_id.label('reporting_fiscal_year'),
        model.submission_id.label('reporting_fiscal_period'),
        model.submission_id.label('ussgl_account_num'),
        model.submission_id.label('dollar_amount_gtas'),
        model.submission_id.label('dollar_amount_broker'),
        model.submission_id.label('dollar_amount_diff'),
        model.submission_id.label('file_b_rows'))