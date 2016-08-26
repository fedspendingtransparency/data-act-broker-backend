import factory
from factory import fuzzy

from dataactcore.models import stagingModels


class AppropriationFactory(factory.Factory):
    class Meta:
        model = stagingModels.Appropriation

    appropriation_id = None
    submission_id = fuzzy.FuzzyInteger(9999)
    job_id = fuzzy.FuzzyInteger(9999)
    row_number = fuzzy.FuzzyInteger(1, 9999)
    adjustments_to_unobligated_cpe = fuzzy.FuzzyDecimal(9999)
    agency_identifier = fuzzy.FuzzyText()
    allocation_transfer_agency = fuzzy.FuzzyText()
    availability_type_code = fuzzy.FuzzyText()
    beginning_period_of_availa = fuzzy.FuzzyText()
    borrowing_authority_amount_cpe = fuzzy.FuzzyDecimal(9999)
    budget_authority_appropria_cpe = fuzzy.FuzzyDecimal(9999)
    budget_authority_available_cpe = fuzzy.FuzzyDecimal(9999)
    budget_authority_unobligat_fyb = fuzzy.FuzzyDecimal(9999)
    contract_authority_amount_cpe = fuzzy.FuzzyDecimal(9999)
    deobligations_recoveries_r_cpe = fuzzy.FuzzyDecimal(9999)
    ending_period_of_availabil = fuzzy.FuzzyText()
    gross_outlay_amount_by_tas_cpe = fuzzy.FuzzyDecimal(9999)
    main_account_code = fuzzy.FuzzyText()
    obligations_incurred_total_cpe = fuzzy.FuzzyDecimal(9999)
    other_budgetary_resources_cpe = fuzzy.FuzzyDecimal(9999)
    spending_authority_from_of_cpe = fuzzy.FuzzyDecimal(9999)
    status_of_budgetary_resour_cpe = fuzzy.FuzzyDecimal(9999)
    sub_account_code = fuzzy.FuzzyText()
    unobligated_balance_cpe = fuzzy.FuzzyDecimal(9999)
    tas = fuzzy.FuzzyText()
    is_first_quarter = False
