import datetime

from collections import OrderedDict
from sqlalchemy import or_, and_, func, null, values, column, cast
from sqlalchemy.orm import outerjoin, column_property
from sqlalchemy.sql.expression import case, literal_column, literal

from dataactcore.models.domainModels import SF133, TASLookup, CGAC, FREC, TASFailedEdits, GTASBOC
from dataactcore.models.lookups import PUBLISH_STATUS_DICT
from dataactcore.models.stagingModels import PublishedObjectClassProgramActivity
from dataactcore.models.jobModels import SubmissionWindowSchedule, Submission

fileb_model = PublishedObjectClassProgramActivity
boc_model = GTASBOC

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
    ('prior_year_adjustment', ['PriorYearAdjustment']),
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
    # Get the submission ID to compare to
    submission_query = session.query(Submission.submission_id). \
        filter_by(reporting_fiscal_year=year, reporting_fiscal_period=period,
                  publish_status_id=PUBLISH_STATUS_DICT['published'])
    if len(agency_code) == 3:
        submission_query.filter_by(cgac_code=agency_code)
    else:
        submission_query.filter_by(frec_code=agency_code)
    submission_id = submission_query.first().submission_id

    ussgl_vals = ussgl_values()

    # Create the CTE of only the laterally displayed USSGL values in the given submission
    ussgl_pub_file_b = ussgl_published_file_b_cte(session, ussgl_vals, submission_id)

    rows = initial_query(session, ussgl_pub_file_b.c, period, year).\
        filter(func.coalesce(boc_model.fiscal_year, year) == year,
               func.coalesce(boc_model.period, period) == period)

    return rows


def ussgl_values():
    """ Create a lateral subquery of VALUES for the USSGLs

        Returns:
            A lateral subquery to get data from
    """
    # Create a list of the values we need to pivot on/display from the USSGLs
    ussgl_list = [fileb_model.ussgl480100_undelivered_or_cpe, fileb_model.ussgl480100_undelivered_or_fyb,
                  fileb_model.ussgl480200_undelivered_or_cpe, fileb_model.ussgl480200_undelivered_or_fyb,
                  fileb_model.ussgl483100_undelivered_or_cpe, fileb_model.ussgl483200_undelivered_or_cpe,
                  fileb_model.ussgl487100_downward_adjus_cpe, fileb_model.ussgl487200_downward_adjus_cpe,
                  fileb_model.ussgl488100_upward_adjustm_cpe, fileb_model.ussgl488200_upward_adjustm_cpe,
                  fileb_model.ussgl490100_delivered_orde_cpe, fileb_model.ussgl490100_delivered_orde_fyb,
                  fileb_model.ussgl490200_delivered_orde_cpe, fileb_model.ussgl490800_authority_outl_cpe,
                  fileb_model.ussgl490800_authority_outl_fyb, fileb_model.ussgl493100_delivered_orde_cpe,
                  fileb_model.ussgl497100_downward_adjus_cpe, fileb_model.ussgl497200_downward_adjus_cpe,
                  fileb_model.ussgl498100_upward_adjustm_cpe, fileb_model.ussgl498200_upward_adjustm_cpe]
    val_data = []
    for ussgl_val in ussgl_list:
        ussgl_name = ussgl_val.name
        ussgl_num = ussgl_name[5:11]
        begin_end = ussgl_name[-1].upper()
        val_data.append((fileb_model.display_tas, fileb_model.disaster_emergency_fund_code, fileb_model.object_class,
                         fileb_model.by_direct_reimbursable_fun, begin_end, ussgl_num, ussgl_val))

    # Create a VALUES subquery. The "lateral" can be explained as: "LATERAL join is like a SQL foreach loop, in which
    # PostgreSQL will iterate over each row in a result set and evaluate a subquery using that row as a parameter"
    # More simply, if there are multiple rows in the VALUES list, it will join on each of them separately and create
    # a semi-pivoted table. It is not the most efficient but file B is a shorter file per submission. This should
    # not be used on larger things like file C
    return values(
        column('display_tas'),
        column('disaster_emergency_fund_code'),
        column('object_class'),
        column('by_direct_reimbursable_fun'),
        column('begin_end_indicator'),
        column('ussgl_account_number'),
        column('dollar_amount'),
        name='test'
    ).data(val_data).lateral()


def ussgl_published_file_b_cte(session, ussgl_vals, submission_id):
    """ Creates the CTE to be used in the main query by performing the lateral join on the VALUES and file B

        Args:
            session: The current DB session
            ussgs_vals: The lateral collection of VALUES defined to retrieve for the given file B
            submission_id: The ID of the submission to get the rest of the data for

        Returns:
            A CTE containing the information relating to file B split up by individual USSGL for the given submission
    """
    cte_query = session.query(
            fileb_model.display_tas,
            fileb_model.allocation_transfer_agency,
            fileb_model.agency_identifier,
            fileb_model.beginning_period_of_availa,
            fileb_model.ending_period_of_availabil,
            fileb_model.availability_type_code,
            fileb_model.main_account_code,
            fileb_model.sub_account_code,
            fileb_model.disaster_emergency_fund_code,
            func.rpad(fileb_model.object_class, 4, '0').label('object_class'),
            fileb_model.by_direct_reimbursable_fun,
            ussgl_vals.c.begin_end_indicator,
            ussgl_vals.c.ussgl_account_number,
            ussgl_vals.c.dollar_amount). \
        join(ussgl_vals, and_(ussgl_vals.c.display_tas == fileb_model.display_tas,
                              ussgl_vals.c.disaster_emergency_fund_code == fileb_model.disaster_emergency_fund_code,
                              ussgl_vals.c.object_class == fileb_model.object_class,
                              ussgl_vals.c.by_direct_reimbursable_fun == fileb_model.by_direct_reimbursable_fun)). \
        filter(fileb_model.submission_id == submission_id).cte('ussgl_pub_file_b')

    return cte_query


def initial_query(session, model, period, year):
    """ Creates the initial query for BOC files.

        Args:
            session: The current DB session
            model: subquery model to get data from
            period: The period for which to get data
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
            model.by_direct_reimbursable_fun,
            model.disaster_emergency_fund_code,
            boc_model.prior_year_adjustment_code.label('prior_year_adjustment'),
            model.begin_end_indicator.label('begin_end'),
            boc_model.fiscal_year.label('reporting_fiscal_year'),
            boc_model.period.label('reporting_fiscal_period'),
            model.ussgl_account_number.label('ussgl_account_num'),
            model.dollar_amount.label('dollar_amount_gtas'),
            (func.coalesce(boc_model.dollar_amount, 0) * case([(boc_model.debit_credit == 'D', 1)], else_=-1)).label('dollar_amount_broker'),
            ((func.coalesce(boc_model.dollar_amount, 0) * case([(boc_model.debit_credit == 'D', 1)], else_=-1)) - model.dollar_amount).label('dollar_amount_diff'),
            model.display_tas.label('file_b_rows')).\
        outerjoin(boc_model, and_(model.display_tas == boc_model.display_tas,
                                  model.disaster_emergency_fund_code == boc_model.disaster_emergency_fund_code,
                                  model.object_class == boc_model.budget_object_class,
                                  model.by_direct_reimbursable_fun == boc_model.reimbursable_flag,
                                  model.begin_end_indicator == boc_model.begin_end,
                                  model.ussgl_account_number == boc_model.ussgl_number))