import re

from collections import OrderedDict
from sqlalchemy import or_, and_, func, values, column
from sqlalchemy.sql.expression import case, literal

from dataactbroker.helpers.filters_helper import tas_agency_filter
from dataactcore.models.domainModels import GTASBOC, TASLookup
from dataactcore.models.lookups import PUBLISH_STATUS_DICT
from dataactcore.models.stagingModels import PublishedObjectClassProgramActivity
from dataactcore.models.jobModels import Submission

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
    """ Request BOC comparison file data

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
        submission_query = submission_query.filter_by(cgac_code=agency_code)
    else:
        submission_query = submission_query.filter_by(frec_code=agency_code)
    submission_id = submission_query.first().submission_id

    # Get the summed values of file B, so we can ignore PAC/PAN's existence and properly compare to GTAS BOC
    summed_pub_b = sum_published_file_b(session, submission_id)

    # Create the VALUES grouping for a lateral join
    ussgl_vals = ussgl_values(summed_pub_b.c)

    # Create the CTE of only the laterally displayed USSGL values in the given submission
    ussgl_pub_file_b = ussgl_published_file_b_cte(session, ussgl_vals, summed_pub_b.c)

    # Get the summed values of GTAS BOC, so we can ignore PYA's existence and properly compare to file B
    summed_gtas_boc = sum_gtas_boc(session, period, year, agency_code)

    # Filter out any rows that have 0 for both the published file B and GTAS BOC totals
    rows = initial_query(session, ussgl_pub_file_b.c, summed_gtas_boc, period, year).\
        filter(or_(ussgl_pub_file_b.c.dollar_amount != 0,
                   func.coalesce(summed_gtas_boc.c.sum_dollar_amount, 0) != 0))

    return rows


def sum_published_file_b(session, submission_id):
    """ Sums the specified published file B to ignore PAC/PAN. Ignores BOC of 0, 00, 000, or 0000

        Args:
            session: The current DB session
            submission_id: The submission ID of the published file B to get data for

        Returns:
            The specified file B data summed to work with
    """

    summed_b = session.query(
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
        func.coalesce(func.upper(fileb_model.prior_year_adjustment), '').label('prior_year_adjustment'),
        func.sum(fileb_model.ussgl480100_undelivered_or_cpe).label('sum_ussgl480100_undelivered_or_cpe'),
        func.sum(fileb_model.ussgl480100_undelivered_or_fyb).label('sum_ussgl480100_undelivered_or_fyb'),
        func.sum(fileb_model.ussgl480110_reinstated_del_cpe).label('sum_ussgl480110_reinstated_del_cpe'),
        func.sum(fileb_model.ussgl480200_undelivered_or_cpe).label('sum_ussgl480200_undelivered_or_cpe'),
        func.sum(fileb_model.ussgl480200_undelivered_or_fyb).label('sum_ussgl480200_undelivered_or_fyb'),
        func.sum(fileb_model.ussgl483100_undelivered_or_cpe).label('sum_ussgl483100_undelivered_or_cpe'),
        func.sum(fileb_model.ussgl483200_undelivered_or_cpe).label('sum_ussgl483200_undelivered_or_cpe'),
        func.sum(fileb_model.ussgl487100_downward_adjus_cpe).label('sum_ussgl487100_downward_adjus_cpe'),
        func.sum(fileb_model.ussgl487200_downward_adjus_cpe).label('sum_ussgl487200_downward_adjus_cpe'),
        func.sum(fileb_model.ussgl488100_upward_adjustm_cpe).label('sum_ussgl488100_upward_adjustm_cpe'),
        func.sum(fileb_model.ussgl488200_upward_adjustm_cpe).label('sum_ussgl488200_upward_adjustm_cpe'),
        func.sum(fileb_model.ussgl490100_delivered_orde_cpe).label('sum_ussgl490100_delivered_orde_cpe'),
        func.sum(fileb_model.ussgl490100_delivered_orde_fyb).label('sum_ussgl490100_delivered_orde_fyb'),
        func.sum(fileb_model.ussgl490110_reinstated_del_cpe).label('sum_ussgl490110_reinstated_del_cpe'),
        func.sum(fileb_model.ussgl490200_delivered_orde_cpe).label('sum_ussgl490200_delivered_orde_cpe'),
        func.sum(fileb_model.ussgl490800_authority_outl_cpe).label('sum_ussgl490800_authority_outl_cpe'),
        func.sum(fileb_model.ussgl490800_authority_outl_fyb).label('sum_ussgl490800_authority_outl_fyb'),
        func.sum(fileb_model.ussgl493100_delivered_orde_cpe).label('sum_ussgl493100_delivered_orde_cpe'),
        func.sum(fileb_model.ussgl497100_downward_adjus_cpe).label('sum_ussgl497100_downward_adjus_cpe'),
        func.sum(fileb_model.ussgl497200_downward_adjus_cpe).label('sum_ussgl497200_downward_adjus_cpe'),
        func.sum(fileb_model.ussgl498100_upward_adjustm_cpe).label('sum_ussgl498100_upward_adjustm_cpe'),
        func.sum(fileb_model.ussgl498200_upward_adjustm_cpe).label('sum_ussgl498200_upward_adjustm_cpe'),
        func.array_agg(fileb_model.row_number).label('row_numbers')
    ).filter(fileb_model.submission_id == submission_id,
             func.rpad(fileb_model.object_class, 4, '0') != '0000').\
        group_by(fileb_model.display_tas, fileb_model.allocation_transfer_agency, fileb_model.agency_identifier,
                 fileb_model.beginning_period_of_availa, fileb_model.ending_period_of_availabil,
                 fileb_model.availability_type_code, fileb_model.main_account_code, fileb_model.sub_account_code,
                 fileb_model.disaster_emergency_fund_code, func.rpad(fileb_model.object_class, 4, '0'),
                 fileb_model.by_direct_reimbursable_fun,
                 func.coalesce(func.upper(fileb_model.prior_year_adjustment), '')).\
        cte('summed_pub_b')

    return summed_b


def ussgl_values(model):
    """ Create a lateral subquery of VALUES for the USSGLs

        Args:
            model: subquery model to get data from

        Returns:
            A lateral subquery to get data from
    """
    # Create a list of the values we need to pivot on/display from the USSGLs
    ussgl_list = [model.sum_ussgl480100_undelivered_or_cpe, model.sum_ussgl480100_undelivered_or_fyb,
                  model.sum_ussgl480110_reinstated_del_cpe, model.sum_ussgl480200_undelivered_or_cpe,
                  model.sum_ussgl480200_undelivered_or_fyb, model.sum_ussgl483100_undelivered_or_cpe,
                  model.sum_ussgl483200_undelivered_or_cpe, model.sum_ussgl487100_downward_adjus_cpe,
                  model.sum_ussgl487200_downward_adjus_cpe, model.sum_ussgl488100_upward_adjustm_cpe,
                  model.sum_ussgl488200_upward_adjustm_cpe, model.sum_ussgl490100_delivered_orde_cpe,
                  model.sum_ussgl490100_delivered_orde_fyb, model.sum_ussgl490110_reinstated_del_cpe,
                  model.sum_ussgl490200_delivered_orde_cpe, model.sum_ussgl490800_authority_outl_cpe,
                  model.sum_ussgl490800_authority_outl_fyb, model.sum_ussgl493100_delivered_orde_cpe,
                  model.sum_ussgl497100_downward_adjus_cpe, model.sum_ussgl497200_downward_adjus_cpe,
                  model.sum_ussgl498100_upward_adjustm_cpe, model.sum_ussgl498200_upward_adjustm_cpe]
    val_data = []
    for ussgl_val in ussgl_list:
        ussgl_name = ussgl_val.name
        ussgl_num = ussgl_name[9:15]
        begin_end = ussgl_name[-1].upper()
        val_data.append(
            (model.display_tas, model.allocation_transfer_agency, model.agency_identifier,
             model.beginning_period_of_availa, model.ending_period_of_availabil, model.availability_type_code,
             model.main_account_code, model.sub_account_code, model.disaster_emergency_fund_code, model.object_class,
             model.by_direct_reimbursable_fun, model.prior_year_adjustment, begin_end, ussgl_num, ussgl_val,
             model.row_numbers))

    # Create a VALUES subquery. The "lateral" can be explained as: "LATERAL join is like a SQL foreach loop, in which
    # PostgreSQL will iterate over each row in a result set and evaluate a subquery using that row as a parameter"
    # More simply, it allows a subquery in the FROM to refer to the columns of the preceding tables. It can be
    # significantly more efficient than introducing a large number of joins to make a query work
    return values(
        column('display_tas'),
        column('allocation_transfer_agency'),
        column('agency_identifier'),
        column('beginning_period_of_availa'),
        column('ending_period_of_availabil'),
        column('availability_type_code'),
        column('.main_account_code'),
        column('sub_account_code'),
        column('disaster_emergency_fund_code'),
        column('object_class'),
        column('by_direct_reimbursable_fun'),
        column('prior_year_adjustment'),
        column('begin_end_indicator'),
        column('ussgl_account_number'),
        column('dollar_amount'),
        column('row_numbers'),
        name='test'
    ).data(val_data).lateral()


def ussgl_published_file_b_cte(session, ussgl_vals, model):
    """ Creates the CTE to be used in the main query by performing the lateral join on the VALUES and file B

        Args:
            session: The current DB session
            ussgl_vals: The lateral collection of VALUES defined to retrieve for the given file B
            model: subquery model to get data from

        Returns:
            A CTE containing the information relating to file B split up by individual USSGL for the given submission
    """
    ussgl_pub_file_b = session.query(
        model.display_tas,
        model.allocation_transfer_agency,
        model.agency_identifier,
        model.beginning_period_of_availa,
        model.ending_period_of_availabil,
        model.availability_type_code,
        model.main_account_code,
        model.sub_account_code,
        model.disaster_emergency_fund_code,
        model.object_class,
        model.by_direct_reimbursable_fun,
        model.prior_year_adjustment,
        ussgl_vals.c.begin_end_indicator,
        ussgl_vals.c.ussgl_account_number,
        ussgl_vals.c.dollar_amount,
        model.row_numbers
    ).join(ussgl_vals, and_(ussgl_vals.c.display_tas == model.display_tas,
                            ussgl_vals.c.disaster_emergency_fund_code == model.disaster_emergency_fund_code,
                            ussgl_vals.c.object_class == model.object_class,
                            ussgl_vals.c.by_direct_reimbursable_fun == model.by_direct_reimbursable_fun,
                            ussgl_vals.c.prior_year_adjustment == model.prior_year_adjustment)).\
        cte('ussgl_pub_file_b')

    return ussgl_pub_file_b


def sum_gtas_boc(session, period, year, agency_code):
    """ Sums the GTAS BOC data for the given year/period/agency to ignore BOC of 999 or 9999

        Args:
            session: The current DB session
            period: The period for which to get data
            year: the year for which to get data
            agency_code: the agency code to filter by

        Returns:
            A CTE containing the summed data for GTAS BOC for the given year/period
    """
    agency_filters = tas_agency_filter(session, agency_code, TASLookup)
    exists_query = session.query(TASLookup).filter(TASLookup.display_tas == boc_model.display_tas,
                                                   or_(*agency_filters)).exists()
    ussgl_list = list(set([re.findall(r'ussgl(\d+)_.*', col.name)[0] for col in fileb_model.__table__.columns if
                           col.name.startswith('ussgl')]))
    sum_boc = session.query(
        boc_model.display_tas,
        boc_model.allocation_transfer_agency,
        boc_model.agency_identifier,
        boc_model.beginning_period_of_availa,
        boc_model.ending_period_of_availabil,
        boc_model.availability_type_code,
        boc_model.main_account_code,
        boc_model.sub_account_code,
        boc_model.disaster_emergency_fund_code,
        boc_model.budget_object_class,
        boc_model.reimbursable_flag,
        boc_model.begin_end,
        func.coalesce(func.upper(boc_model.prior_year_adjustment_code), '').label('prior_year_adjustment'),
        boc_model.ussgl_number,
        func.sum(boc_model.dollar_amount * case([(boc_model.debit_credit == 'D', 1)], else_=-1)).
        label('sum_dollar_amount')
    ).filter(boc_model.fiscal_year == year, boc_model.period == period,
             func.coalesce(func.rpad(boc_model.budget_object_class, 4, '9'), '') != '9999',
             boc_model.ussgl_number.in_(ussgl_list), exists_query).\
        group_by(boc_model.display_tas, boc_model.allocation_transfer_agency, boc_model.agency_identifier,
                 boc_model.beginning_period_of_availa, boc_model.ending_period_of_availabil,
                 boc_model.availability_type_code, boc_model.main_account_code, boc_model.sub_account_code,
                 boc_model.disaster_emergency_fund_code, boc_model.budget_object_class,
                 boc_model.reimbursable_flag, boc_model.begin_end, boc_model.ussgl_number,
                 func.coalesce(func.upper(boc_model.prior_year_adjustment_code), '')).\
        cte('summed_gtas_boc')
    return sum_boc


def initial_query(session, model, summed_boc_model, period, year):
    """ Creates the initial query for BOC files.

        Args:
            session: The current DB session
            model: subquery model to get data from
            summed_boc_model: subquery model of the BOC data summed together
            period: period to display in the file
            year: year to display in the file

        Returns:
            The base query.
    """
    return session.query(
        func.coalesce(model.display_tas, summed_boc_model.c.display_tas).label('display_tas'),
        func.coalesce(model.allocation_transfer_agency,
                      summed_boc_model.c.allocation_transfer_agency).label('allocation_transfer_agency'),
        func.coalesce(model.agency_identifier, summed_boc_model.c.agency_identifier).label('agency_identifier'),
        func.coalesce(model.beginning_period_of_availa,
                      summed_boc_model.c.beginning_period_of_availa).label('beginning_period_of_availa'),
        func.coalesce(model.ending_period_of_availabil,
                      summed_boc_model.c.ending_period_of_availabil).label('ending_period_of_availabil'),
        func.coalesce(model.availability_type_code,
                      summed_boc_model.c.availability_type_code).label('availability_type_code'),
        func.coalesce(model.main_account_code, summed_boc_model.c.main_account_code).label('main_account_code'),
        func.coalesce(model.sub_account_code, summed_boc_model.c.sub_account_code).label('sub_account_code'),
        func.coalesce(model.object_class, summed_boc_model.c.budget_object_class).label('object_class'),
        func.coalesce(model.by_direct_reimbursable_fun,
                      summed_boc_model.c.reimbursable_flag).label('by_direct_reimbursable_fun'),
        func.coalesce(model.disaster_emergency_fund_code,
                      summed_boc_model.c.disaster_emergency_fund_code).label('disaster_emergency_fund_code'),
        func.coalesce(model.prior_year_adjustment,
                      summed_boc_model.c.prior_year_adjustment).label('prior_year_adjustment'),
        func.coalesce(model.begin_end_indicator, summed_boc_model.c.begin_end).label('begin_end'),
        literal(year).label('reporting_fiscal_year'),
        literal(period).label('reporting_fiscal_period'),
        func.coalesce(model.ussgl_account_number, summed_boc_model.c.ussgl_number).label('ussgl_account_num'),
        func.coalesce(summed_boc_model.c.sum_dollar_amount, 0).label('dollar_amount_gtas'),
        func.coalesce(model.dollar_amount, 0).label('dollar_amount_broker'),
        (func.coalesce(summed_boc_model.c.sum_dollar_amount, 0) - func.coalesce(model.dollar_amount, 0)).
        label('dollar_amount_diff'),
        model.row_numbers.label('file_b_rows')
    ).join(summed_boc_model,
           and_(model.display_tas == summed_boc_model.c.display_tas,
                model.disaster_emergency_fund_code == summed_boc_model.c.disaster_emergency_fund_code,
                model.object_class == summed_boc_model.c.budget_object_class,
                model.by_direct_reimbursable_fun == summed_boc_model.c.reimbursable_flag,
                model.begin_end_indicator == summed_boc_model.c.begin_end,
                model.ussgl_account_number == summed_boc_model.c.ussgl_number,
                model.prior_year_adjustment == summed_boc_model.c.prior_year_adjustment),
           full=True)
