from collections import OrderedDict

from dataactcore.models.domainModels import SF133
from dataactcore.models.stagingModels import Appropriation

file_model = SF133
staging_model = Appropriation

mapping = OrderedDict([
    ('AdjustmentsToUnobligatedBalanceBroughtForward_CPE', 'adjustments_to_unobligated_cpe'),
    ('AgencyIdentifier', 'agency_identifier'),
    ('AllocationTransferAgencyIdentifier', 'allocation_transfer_agency'),
    ('AvailabilityTypeCode', 'availability_type_code'),
    ('BeginningPeriodOfAvailability', 'beginning_period_of_availa'),
    ('BorrowingAuthorityAmountTotal_CPE', 'borrowing_authority_amount_cpe'),
    ('BudgetAuthorityAppropriatedAmount_CPE', 'budget_authority_appropria_cpe'),
    ('TotalBudgetaryResources_CPE', 'total_budgetary_resources_cpe'),
    ('BudgetAuthorityUnobligatedBalanceBroughtForward_FYB', 'budget_authority_unobligat_fyb'),
    ('ContractAuthorityAmountTotal_CPE', 'contract_authority_amount_cpe'),
    ('DeobligationsRecoveriesRefundsByTAS_CPE', 'deobligations_recoveries_r_cpe'),
    ('EndingPeriodOfAvailability', 'ending_period_of_availabil'),
    ('GrossOutlayAmountByTAS_CPE', 'gross_outlay_amount_by_tas_cpe'),
    ('MainAccountCode', 'main_account_code'),
    ('ObligationsIncurredTotalByTAS_CPE', 'obligations_incurred_total_cpe'),
    ('OtherBudgetaryResourcesAmount_CPE', 'other_budgetary_resources_cpe'),
    ('SpendingAuthorityfromOffsettingCollectionsAmountTotal_CPE', 'spending_authority_from_of_cpe'),
    ('StatusOfBudgetaryResourcesTotal_CPE', 'status_of_budgetary_resour_cpe'),
    ('SubAccountCode', 'sub_account_code'),
    ('UnobligatedBalance_CPE', 'unobligated_balance_cpe')
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
    rows = initial_query(session).\
        filter(file_model.agency_identifier == agency_code).\
        filter(file_model.period == period).\
        filter(file_model.fiscal_year == year)

    # Slice the final query
    rows = rows.slice(page_start, page_stop)

    return rows


def initial_query(session):
    """ Creates the initial query for D2 files.

        Args:
            session: The current DB session

        Returns:
            The base query (a select from the PublishedAwardFinancialAssistance table with the specified columns).
    """
    return session.query(
        file_model.amount.label('adjustments_to_unobligated_cpe'),
        file_model.agency_identifier,
        file_model.allocation_transfer_agency,
        file_model.availability_type_code,
        file_model.beginning_period_of_availa,
        file_model.amount.label('borrowing_authority_amount_cpe'),
        file_model.amount.label('budget_authority_appropria_cpe'),
        file_model.amount.label('total_budgetary_resources_cpe'),
        file_model.amount.label('budget_authority_unobligat_fyb'),
        file_model.amount.label('contract_authority_amount_cpe'),
        file_model.amount.label('deobligations_recoveries_r_cpe'),
        file_model.ending_period_of_availabil,
        file_model.amount.label('gross_outlay_amount_by_tas_cpe'),
        file_model.main_account_code,
        file_model.amount.label('obligations_incurred_total_cpe'),
        file_model.amount.label('other_budgetary_resources_cpe'),
        file_model.amount.label('spending_authority_from_of_cpe'),
        file_model.amount.label('status_of_budgetary_resour_cpe'),
        file_model.sub_account_code,
        file_model.amount.label('unobligated_balance_cpe'))
