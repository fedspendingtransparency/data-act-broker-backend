from collections import OrderedDict
from sqlalchemy import or_, and_, func
from sqlalchemy.orm import outerjoin
from sqlalchemy.sql.expression import case

from dataactcore.models.domainModels import SF133, TASLookup, CGAC, FREC

gtas_model = SF133
tas_model = TASLookup

mapping = OrderedDict([
    ('AllocationTransferAgencyIdentifier', 'allocation_transfer_agency'),
    ('AgencyIdentifier', 'agency_identifier'),
    ('BeginningPeriodOfAvailability', 'beginning_period_of_availa'),
    ('EndingPeriodOfAvailability', 'ending_period_of_availabil'),
    ('AvailabilityTypeCode', 'availability_type_code'),
    ('MainAccountCode', 'main_account_code'),
    ('SubAccountCode', 'sub_account_code'),
    ('TotalBudgetaryResources_CPE', 'total_budgetary_resources_cpe'),
    ('BudgetAuthorityAppropriatedAmount_CPE', 'budget_authority_appropria_cpe'),
    ('BudgetAuthorityUnobligatedBalanceBroughtForward_FYB', 'budget_authority_unobligat_fyb'),
    ('AdjustmentsToUnobligatedBalanceBroughtForward_CPE', 'adjustments_to_unobligated_cpe'),
    ('OtherBudgetaryResourcesAmount_CPE', 'other_budgetary_resources_cpe'),
    ('ContractAuthorityAmountTotal_CPE', 'contract_authority_amount_cpe'),
    ('BorrowingAuthorityAmountTotal_CPE', 'borrowing_authority_amount_cpe'),
    ('SpendingAuthorityfromOffsettingCollectionsAmountTotal_CPE', 'spending_authority_from_of_cpe'),
    ('StatusOfBudgetaryResourcesTotal_CPE', 'status_of_budgetary_resour_cpe'),
    ('ObligationsIncurredTotalByTAS_CPE', 'obligations_incurred_total_cpe'),
    ('GrossOutlayAmountByTAS_CPE', 'gross_outlay_amount_by_tas_cpe'),
    ('UnobligatedBalance_CPE', 'unobligated_balance_cpe'),
    ('DeobligationsRecoveriesRefundsByTAS_CPE', 'deobligations_recoveries_r_cpe')
])
db_columns = [val for key, val in mapping.items()]


def query_data(session, agency_code, period, year, page_start, page_stop):
    """ Request A file data

        Args:
            session: DB session
            agency_code: FREC or CGAC code for generation
            period: The period for which to get GTAS data
            year: The year for which to get GTAS data
            page_start: Beginning of pagination
            page_stop: End of pagination

        Returns:
            The rows using the provided dates and page size for the given agency.
    """
    # set a boolean to determine if the original agency code is frec or cgac
    frec_provided = len(agency_code) == 4
    tas_gtas = tas_gtas_combo(session, period, year)
    # Make a list of FRECs to compare to for 011 AID entries
    frec_list = []
    if not frec_provided:
        frec_list = session.query(FREC.frec_code).select_from(outerjoin(CGAC, FREC, CGAC.cgac_id == FREC.cgac_id)).\
            filter(CGAC.cgac_code == agency_code).all()
    # Group agencies together that need to be grouped
    agency_array = []
    if agency_code == '097':
        agency_array = ['017', '021', '057', '097']
    elif agency_code == '1601':
        agency_array = ['1601', '016']
    elif agency_code == '1125':
        agency_array = ['1125', '011']

    # Save the ATA filter
    agency_filters = []
    if not agency_array:
        agency_filters.append(tas_gtas.c.allocation_transfer_agency == agency_code)
    else:
        agency_filters.append(tas_gtas.c.allocation_transfer_agency.in_(agency_array))

    # Save the AID filter
    if agency_code == '097' and not frec_provided:
        agency_filters.append(and_(tas_gtas.c.allocation_transfer_agency.is_(None),
                                   tas_gtas.c.agency_identifier.in_(agency_array)))
    elif not frec_provided:
        agency_filters.append(and_(tas_gtas.c.allocation_transfer_agency.is_(None),
                                   tas_gtas.c.agency_identifier == agency_code))
    else:
        agency_filters.append(and_(tas_gtas.c.allocation_transfer_agency.is_(None),
                                   tas_gtas.c.fr_entity_type == agency_code))

    # If we're checking a CGAC, we want to filter on all of the related FRECs for AID 011, otherwise just filter on
    # that FREC
    if frec_list:
        agency_filters.append(and_(tas_gtas.c.allocation_transfer_agency.is_(None),
                                   tas_gtas.c.agency_identifier == '011',
                                   tas_gtas.c.fr_entity_type.in_(frec_list)))
    elif not frec_provided:
        agency_filters.append(and_(tas_gtas.c.allocation_transfer_agency.is_(None),
                                   tas_gtas.c.agency_identifier == '011',
                                   tas_gtas.c.fr_entity_type == agency_code))

    rows = initial_query(session, tas_gtas.c).\
        filter(func.coalesce(tas_gtas.c.financial_indicator2, '') != 'F').\
        filter(or_(*agency_filters)).\
        group_by(tas_gtas.c.allocation_transfer_agency,
                 tas_gtas.c.agency_identifier,
                 tas_gtas.c.beginning_period_of_availa,
                 tas_gtas.c.ending_period_of_availabil,
                 tas_gtas.c.availability_type_code,
                 tas_gtas.c.main_account_code,
                 tas_gtas.c.sub_account_code)

    # Slice the final query
    rows = rows.slice(page_start, page_stop)

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
            tas_model.fr_entity_type.label('fr_entity_type')).\
        join(tas_model, gtas_model.tas == func.concat(func.coalesce(tas_model.allocation_transfer_agency, '000'),
                                                      func.coalesce(tas_model.agency_identifier, '000'),
                                                      func.coalesce(tas_model.beginning_period_of_availa, '0000'),
                                                      func.coalesce(tas_model.ending_period_of_availabil, '0000'),
                                                      func.coalesce(tas_model.availability_type_code, ' '),
                                                      func.coalesce(tas_model.main_account_code, '0000'),
                                                      func.coalesce(tas_model.sub_account_code, '000'))).\
        filter(gtas_model.period == period).\
        filter(gtas_model.fiscal_year == year)
    return query.cte('tas_gtas')


def initial_query(session, model):
    """ Creates the initial query for D2 files.

        Args:
            session: The current DB session
            model: subquery model to get data from

        Returns:
            The base query (a select from the PublishedAwardFinancialAssistance table with the specified columns).
    """
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
        func.sum(case([(and_(model.line >= 1010, model.line <= 1042), model.amount)],
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
        func.sum(case([(model.line.in_([1021, 1033]), model.amount)], else_=0)).label('deobligations_recoveries_r_cpe'))
