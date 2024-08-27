import datetime

from collections import OrderedDict
from sqlalchemy import or_, and_, func, null
from sqlalchemy.sql.expression import case, literal_column

from dataactbroker.helpers.filters_helper import tas_agency_filter
from dataactcore.interfaces.function_bag import get_utc_now
from dataactcore.models.domainModels import SF133, TASLookup, TASFailedEdits
from dataactcore.models.jobModels import SubmissionWindowSchedule

gtas_model = SF133
tas_model = TASLookup
fail_model = TASFailedEdits

mapping = OrderedDict([
    ('allocation_transfer_agency', ['AllocationTransferAgencyIdentifier']),
    ('agency_identifier', ['AgencyIdentifier']),
    ('beginning_period_of_availa', ['BeginningPeriodOfAvailability']),
    ('ending_period_of_availabil', ['EndingPeriodOfAvailability']),
    ('availability_type_code', ['AvailabilityTypeCode']),
    ('main_account_code', ['MainAccountCode']),
    ('sub_account_code', ['SubAccountCode']),
    ('total_budgetary_resources_cpe', ['TotalBudgetaryResources_CPE']),
    ('budget_authority_appropria_cpe', ['BudgetAuthorityAppropriatedAmount_CPE']),
    ('budget_authority_unobligat_fyb', ['BudgetAuthorityUnobligatedBalanceBroughtForward_FYB']),
    ('adjustments_to_unobligated_cpe', ['AdjustmentsToUnobligatedBalanceBroughtForward_CPE']),
    ('other_budgetary_resources_cpe', ['OtherBudgetaryResourcesAmount_CPE']),
    ('contract_authority_amount_cpe', ['ContractAuthorityAmountTotal_CPE']),
    ('borrowing_authority_amount_cpe', ['BorrowingAuthorityAmountTotal_CPE']),
    ('spending_authority_from_of_cpe', ['SpendingAuthorityfromOffsettingCollectionsAmountTotal_CPE']),
    ('status_of_budgetary_resour_cpe', ['StatusOfBudgetaryResourcesTotal_CPE']),
    ('obligations_incurred_total_cpe', ['ObligationsIncurredTotalByTAS_CPE']),
    ('gross_outlay_amount_by_tas_cpe', ['GrossOutlayAmountByTAS_CPE']),
    ('unobligated_balance_cpe', ['UnobligatedBalance_CPE']),
    ('deobligations_recoveries_r_cpe', ['DeobligationsRecoveriesRefundsOfPriorYearByTAS_CPE']),
    ('gtas_status', ['GTASStatus'])
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
    tas_gtas = tas_gtas_combo(session, period, year)
    tas_gtas_fail = failed_edits_details(session, tas_gtas, period, year)
    agency_filters = tas_agency_filter(session, agency_code, tas_gtas_fail.c)

    rows = initial_query(session, tas_gtas_fail.c, year).\
        filter(func.coalesce(tas_gtas_fail.c.financial_indicator2, '') != 'F').\
        filter(or_(*agency_filters)).\
        group_by(tas_gtas_fail.c.allocation_transfer_agency,
                 tas_gtas_fail.c.agency_identifier,
                 tas_gtas_fail.c.beginning_period_of_availa,
                 tas_gtas_fail.c.ending_period_of_availabil,
                 tas_gtas_fail.c.availability_type_code,
                 tas_gtas_fail.c.main_account_code,
                 tas_gtas_fail.c.sub_account_code,
                 tas_gtas_fail.c.gtas_status)

    return rows


def tas_gtas_combo(session, period, year):
    """ Creates a combined list of TAS and GTAS data filtered by the given period/year

        Args:
            session: DB session
            period: The period for which to get GTAS data
            year: The year for which to get GTAS data

        Returns:
            A WITH clause to use with other queries
    """
    query = session.query(
        gtas_model.allocation_transfer_agency.label('allocation_transfer_agency'),
        gtas_model.agency_identifier.label('agency_identifier'),
        gtas_model.beginning_period_of_availa.label('beginning_period_of_availa'),
        gtas_model.ending_period_of_availabil.label('ending_period_of_availabil'),
        gtas_model.availability_type_code.label('availability_type_code'),
        gtas_model.main_account_code.label('main_account_code'),
        gtas_model.sub_account_code.label('sub_account_code'),
        gtas_model.amount.label('amount'),
        gtas_model.line.label('line'),
        tas_model.financial_indicator2.label('financial_indicator2'),
        tas_model.fr_entity_type.label('fr_entity_type'),
        gtas_model.tas.label('tas')).\
        join(tas_model, gtas_model.tas == tas_model.tas).\
        filter(gtas_model.period == period).\
        filter(gtas_model.fiscal_year == year)
    return query.cte('tas_gtas')


def failed_edits_details(session, tas_gtas, period, year):
    """ Creates a combined list of tas_failed_edits details

        Args:
            session: DB session
            tas_gtas: A CTE containing the filtered tas_gtas models
            period: The period for which to get GTAS data
            year: The year for which to get GTAS data

        Returns:
            A WITH clause to use with other queries
    """
    submission_period = session.query(SubmissionWindowSchedule).filter_by(period=period, year=year).one_or_none()
    # If submission period doesn't exist, the gtas_status will always be blank
    if not submission_period:
        query = session.query(tas_gtas, null().label('gtas_status'))
    elif submission_period.period_start > get_utc_now():
        query = session.query(tas_gtas, literal_column("'GTAS window open'").label('gtas_status'))
    else:
        has_period = session.query(fail_model).filter_by(period=period, fiscal_year=year).first() is not None
        # if the period doesn't exist in the TASFailingEdits model but does exist in our list, leave it blank
        if not has_period:
            query = session.query(tas_gtas, null().label('gtas_status'))
        else:
            # We need this subquery to select only the highest "tier" of each failure
            subquery = session.query(fail_model.severity, fail_model.approved_override_exists,
                                     fail_model.atb_submission_status, fail_model.tas,
                                     func.row_number().over(fail_model.tas,
                                                            order_by=case([
                                                                (fail_model.atb_submission_status == 'F', 1),
                                                                (fail_model.atb_submission_status == 'E', 2),
                                                                (fail_model.atb_submission_status == 'P', 3),
                                                                (fail_model.atb_submission_status == 'C', 4),
                                                            ], else_=5)).label('row')).\
                filter(func.upper(fail_model.severity) == 'FATAL',
                       fail_model.period == period,
                       fail_model.fiscal_year == year).subquery()
            grouped_fail = session.query(subquery).filter(subquery.c.row == 1).cte('grouped_fail')

            query = session.query(
                tas_gtas,
                case([
                    (and_(func.upper(grouped_fail.c.severity) == 'FATAL',
                          grouped_fail.c.approved_override_exists.is_(False)),
                        literal_column("'failed fatal edit - no override'")),
                    (and_(grouped_fail.c.atb_submission_status == 'F',
                          func.upper(grouped_fail.c.severity) == 'FATAL',
                          grouped_fail.c.approved_override_exists.is_(True)),
                        literal_column("'failed fatal edit - override'")),
                    (and_(grouped_fail.c.atb_submission_status == 'E',
                          func.upper(grouped_fail.c.severity) == 'FATAL',
                          grouped_fail.c.approved_override_exists.is_(True)),
                        literal_column("'passed required edits - override'")),
                    (and_(grouped_fail.c.atb_submission_status == 'P',
                          func.upper(grouped_fail.c.severity) == 'FATAL',
                          grouped_fail.c.approved_override_exists.is_(True)),
                        literal_column("'pending certification - override'")),
                    (and_(grouped_fail.c.atb_submission_status == 'C',
                          func.upper(grouped_fail.c.severity) == 'FATAL',
                          grouped_fail.c.approved_override_exists.is_(True)),
                        literal_column("'certified - override'")),
                    (grouped_fail.c.severity.isnot(None), literal_column("'passed required edits - override'"))
                ], else_=literal_column("'passed required edits'")).label('gtas_status')).\
                join(grouped_fail, and_(tas_gtas.c.tas == grouped_fail.c.tas), isouter=True)
    return query.cte('tas_gtas_fail')


def initial_query(session, model, year):
    """ Creates the initial query for A files.

        Args:
            session: The current DB session
            model: subquery model to get data from
            year: the year for which to get data

        Returns:
            The base query (a select from the tas/gtas tables with the specified columns).
    """
    budget_authority_line_max = 1042 if year <= 2020 else 1067
    return session.query(
        model.allocation_transfer_agency,
        model.agency_identifier,
        model.beginning_period_of_availa,
        model.ending_period_of_availabil,
        model.availability_type_code,
        model.main_account_code,
        model.sub_account_code,
        func.sum(case([(model.line == 1910, model.amount)], else_=0)).label('total_budgetary_resources_cpe'),
        func.sum(case([(model.line.in_([1160, 1180, 1260, 1280]), model.amount)],
                      else_=0)).label('budget_authority_appropria_cpe'),
        func.sum(case([(model.line == 1000, model.amount)], else_=0)).label('budget_authority_unobligat_fyb'),
        func.sum(case([(and_(model.line >= 1010, model.line <= budget_authority_line_max), model.amount)],
                      else_=0)).label('adjustments_to_unobligated_cpe'),
        func.sum(case([(model.line.in_([1540, 1640, 1340, 1440, 1750, 1850]), model.amount)],
                      else_=0)).label('other_budgetary_resources_cpe'),
        func.sum(case([(model.line.in_([1540, 1640]), model.amount)], else_=0)).label('contract_authority_amount_cpe'),
        func.sum(case([(model.line.in_([1340, 1440]), model.amount)], else_=0)).label('borrowing_authority_amount_cpe'),
        func.sum(case([(model.line.in_([1750, 1850]), model.amount)], else_=0)).label('spending_authority_from_of_cpe'),
        func.sum(case([(model.line == 2500, model.amount)], else_=0)).label('status_of_budgetary_resour_cpe'),
        func.sum(case([(model.line == 2190, model.amount)], else_=0)).label('obligations_incurred_total_cpe'),
        func.sum(case([(model.line == 3020, model.amount)], else_=0)).label('gross_outlay_amount_by_tas_cpe'),
        func.sum(case([(model.line == 2490, model.amount)], else_=0)).label('unobligated_balance_cpe'),
        func.sum(case([(model.line.in_([1021, 1033]), model.amount)], else_=0)).label('deobligations_recoveries_r_cpe'),
        model.gtas_status.label('gtas_status'))
