from sqlalchemy import Column, Integer, Text, Numeric, Index, Boolean
from dataactcore.models.validationBase import Base


def concatTas(context):
    """Create a concatenated TAS string for insert into database."""
    tas1 = context.current_parameters['allocation_transfer_agency']
    tas1 = tas1 if tas1 else '000'
    tas2 = context.current_parameters['agency_identifier']
    tas2 = tas2 if tas2 else '000'
    tas3 = context.current_parameters['beginning_period_of_availa']
    tas3 = tas3 if tas3 else '0000'
    tas4 = context.current_parameters['ending_period_of_availabil']
    tas4 = tas4 if tas4 else '0000'
    tas5 = context.current_parameters['availability_type_code']
    tas5 = tas5 if tas5 else ' '
    tas6 = context.current_parameters['main_account_code']
    tas6 = tas6 if tas6 else '0000'
    tas7 = context.current_parameters['sub_account_code']
    tas7 = tas7 if tas7 else '000'
    tas = '{}{}{}{}{}{}{}'.format(tas1, tas2, tas3, tas4, tas5, tas6, tas7)
    return tas


class Appropriation(Base):
    """Model for the appropriation table."""
    __tablename__ = "appropriation"

    appropriation_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, nullable=False, index=True)
    job_id = Column(Integer, nullable=False, index=True)
    row_number = Column(Integer, nullable=False)
    adjustments_to_unobligated_cpe = Column(Numeric)
    agency_identifier = Column(Text)
    allocation_transfer_agency = Column(Text)
    availability_type_code = Column(Text)
    beginning_period_of_availa = Column(Text)
    borrowing_authority_amount_cpe = Column(Numeric)
    budget_authority_appropria_cpe = Column(Numeric)
    budget_authority_available_cpe = Column(Numeric)
    budget_authority_unobligat_fyb = Column(Numeric)
    contract_authority_amount_cpe = Column(Numeric)
    deobligations_recoveries_r_cpe = Column(Numeric)
    ending_period_of_availabil = Column(Text)
    gross_outlay_amount_by_tas_cpe = Column(Numeric)
    main_account_code = Column(Text)
    obligations_incurred_total_cpe = Column(Numeric)
    other_budgetary_resources_cpe = Column(Numeric)
    spending_authority_from_of_cpe = Column(Numeric)
    status_of_budgetary_resour_cpe = Column(Numeric)
    sub_account_code = Column(Text)
    unobligated_balance_cpe = Column(Numeric)
    tas = Column(Text, index=True, nullable=False, default=concatTas, onupdate=concatTas)
    valid_record = Column(Boolean, nullable= False, default=True, server_default="True")
    is_first_quarter = Column(Boolean, nullable=False, default=False, server_default="False")

    def __init__(self, **kwargs):
        # broker is set up to ignore extra columns in submitted data
        # so get rid of any extraneous kwargs before instantiating
        cleanKwargs = {k: v for k, v in kwargs.items() if hasattr(self, k)}
        super(Appropriation, self).__init__(**cleanKwargs)

class ObjectClassProgramActivity(Base):
    """Model for the object_class_program_activity table."""
    __tablename__ = "object_class_program_activity"

    object_class_program_activity_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, nullable=False, index=True)
    job_id = Column(Integer, nullable=False, index=True)
    row_number = Column(Integer, nullable=False)
    agency_identifier = Column(Text)
    allocation_transfer_agency = Column(Text)
    availability_type_code = Column(Text)
    beginning_period_of_availa = Column(Text)
    by_direct_reimbursable_fun = Column(Text)
    deobligations_recov_by_pro_cpe = Column(Numeric)
    ending_period_of_availabil = Column(Text)
    gross_outlay_amount_by_pro_cpe = Column(Numeric)
    gross_outlay_amount_by_pro_fyb = Column(Numeric)
    gross_outlays_delivered_or_cpe = Column(Numeric)
    gross_outlays_delivered_or_fyb = Column(Numeric)
    gross_outlays_undelivered_cpe = Column(Numeric)
    gross_outlays_undelivered_fyb = Column(Numeric)
    main_account_code = Column(Text)
    object_class = Column(Text)
    obligations_delivered_orde_cpe = Column(Numeric)
    obligations_delivered_orde_fyb = Column(Numeric)
    obligations_incurred_by_pr_cpe = Column(Numeric)
    obligations_undelivered_or_cpe = Column(Numeric)
    obligations_undelivered_or_fyb = Column(Numeric)
    program_activity_code = Column(Text)
    program_activity_name = Column(Text)
    sub_account_code = Column(Text)
    ussgl480100_undelivered_or_cpe = Column(Numeric)
    ussgl480100_undelivered_or_fyb = Column(Numeric)
    ussgl480200_undelivered_or_cpe = Column(Numeric)
    ussgl480200_undelivered_or_fyb = Column(Numeric)
    ussgl483100_undelivered_or_cpe = Column(Numeric)
    ussgl483200_undelivered_or_cpe = Column(Numeric)
    ussgl487100_downward_adjus_cpe = Column(Numeric)
    ussgl487200_downward_adjus_cpe = Column(Numeric)
    ussgl488100_upward_adjustm_cpe = Column(Numeric)
    ussgl488200_upward_adjustm_cpe = Column(Numeric)
    ussgl490100_delivered_orde_cpe = Column(Numeric)
    ussgl490100_delivered_orde_fyb = Column(Numeric)
    ussgl490200_delivered_orde_cpe = Column(Numeric)
    ussgl490800_authority_outl_cpe = Column(Numeric)
    ussgl490800_authority_outl_fyb = Column(Numeric)
    ussgl493100_delivered_orde_cpe = Column(Numeric)
    ussgl497100_downward_adjus_cpe = Column(Numeric)
    ussgl497200_downward_adjus_cpe = Column(Numeric)
    ussgl498100_upward_adjustm_cpe = Column(Numeric)
    ussgl498200_upward_adjustm_cpe = Column(Numeric)
    tas = Column(Text, nullable=False, default=concatTas, onupdate=concatTas)
    valid_record = Column(Boolean, nullable=False, default=True, server_default="True")
    is_first_quarter = Column(Boolean, nullable=False, default=False, server_default="False")

    def __init__(self, **kwargs):
        # broker is set up to ignore extra columns in submitted data
        # so get rid of any extraneous kwargs before instantiating
        cleanKwargs = {k: v for k, v in kwargs.items() if hasattr(self, k)}
        super(ObjectClassProgramActivity, self).__init__(**cleanKwargs)

Index("ix_oc_pa_tas_oc_pa",
      ObjectClassProgramActivity.tas,
      ObjectClassProgramActivity.object_class,
      ObjectClassProgramActivity.program_activity_code,
      unique=False)

class AwardFinancial(Base):
    """Model for the award_financial table."""
    __tablename__ = "award_financial"

    award_financial_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, nullable=False, index=True)
    job_id = Column(Integer, nullable=False, index=True)
    row_number = Column(Integer, nullable=False)
    agency_identifier = Column(Text)
    allocation_transfer_agency = Column(Text)
    availability_type_code = Column(Text)
    beginning_period_of_availa = Column(Text)
    by_direct_reimbursable_fun = Column(Text)
    deobligations_recov_by_awa_cpe = Column(Numeric)
    ending_period_of_availabil = Column(Text)
    fain = Column(Text, index=True)
    gross_outlay_amount_by_awa_cpe = Column(Numeric)
    gross_outlay_amount_by_awa_fyb = Column(Numeric)
    gross_outlays_delivered_or_cpe = Column(Numeric)
    gross_outlays_delivered_or_fyb = Column(Numeric)
    gross_outlays_undelivered_cpe = Column(Numeric)
    gross_outlays_undelivered_fyb = Column(Numeric)
    main_account_code = Column(Text)
    object_class = Column(Text)
    obligations_delivered_orde_cpe = Column(Numeric)
    obligations_delivered_orde_fyb = Column(Numeric)
    obligations_incurred_byawa_cpe = Column(Numeric)
    obligations_undelivered_or_cpe = Column(Numeric)
    obligations_undelivered_or_fyb = Column(Numeric)
    parent_award_id = Column(Text)
    piid = Column(Text, index=True)
    program_activity_code = Column(Text)
    program_activity_name = Column(Text)
    sub_account_code = Column(Text)
    transaction_obligated_amou = Column(Numeric)
    uri = Column(Text, index=True)
    ussgl480100_undelivered_or_cpe = Column(Numeric)
    ussgl480100_undelivered_or_fyb = Column(Numeric)
    ussgl480200_undelivered_or_cpe = Column(Numeric)
    ussgl480200_undelivered_or_fyb = Column(Numeric)
    ussgl483100_undelivered_or_cpe = Column(Numeric)
    ussgl483200_undelivered_or_cpe = Column(Numeric)
    ussgl487100_downward_adjus_cpe = Column(Numeric)
    ussgl487200_downward_adjus_cpe = Column(Numeric)
    ussgl488100_upward_adjustm_cpe = Column(Numeric)
    ussgl488200_upward_adjustm_cpe = Column(Numeric)
    ussgl490100_delivered_orde_cpe = Column(Numeric)
    ussgl490100_delivered_orde_fyb = Column(Numeric)
    ussgl490200_delivered_orde_cpe = Column(Numeric)
    ussgl490800_authority_outl_cpe = Column(Numeric)
    ussgl490800_authority_outl_fyb = Column(Numeric)
    ussgl493100_delivered_orde_cpe = Column(Numeric)
    ussgl497100_downward_adjus_cpe = Column(Numeric)
    ussgl497200_downward_adjus_cpe = Column(Numeric)
    ussgl498100_upward_adjustm_cpe = Column(Numeric)
    ussgl498200_upward_adjustm_cpe = Column(Numeric)
    tas = Column(Text, nullable=False, default=concatTas, onupdate=concatTas)
    valid_record = Column(Boolean, nullable=False, default=True, server_default="True")
    is_first_quarter = Column(Boolean, nullable=False, default=False, server_default="False")

    def __init__(self, **kwargs):
        # broker is set up to ignore extra columns in submitted data
        # so get rid of any extraneous kwargs before instantiating
        cleanKwargs = {k: v for k, v in kwargs.items() if hasattr(self, k)}
        super(AwardFinancial, self).__init__(**cleanKwargs)

Index("ix_award_financial_tas_oc_pa",
      AwardFinancial.tas,
      AwardFinancial.object_class,
      AwardFinancial.program_activity_code,
      unique=False)

class AwardFinancialAssistance(Base):
    """Model for the award_financial_assistance table."""
    __tablename__ = "award_financial_assistance"

    award_financial_assistance_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, nullable=False, index=True)
    job_id = Column(Integer, nullable=False, index=True)
    row_number = Column(Integer, nullable=False)
    action_date = Column(Text)
    action_type = Column(Text)
    assistance_type = Column(Text)
    award_description = Column(Text)
    awardee_or_recipient_legal = Column(Text)
    awardee_or_recipient_uniqu = Column(Text)
    awarding_agency_code = Column(Text)
    awarding_agency_name = Column(Text)
    awarding_office_code = Column(Text)
    awarding_office_name = Column(Text)
    awarding_sub_tier_agency_c = Column(Text)
    awarding_sub_tier_agency_n = Column(Text)
    award_modification_amendme = Column(Text)
    business_funds_indicator = Column(Text)
    business_types = Column(Text)
    cfda_number = Column(Text)
    cfda_title = Column(Text)
    correction_late_delete_ind = Column(Text)
    face_value_loan_guarantee = Column(Numeric)
    fain = Column(Text, index=True)
    federal_action_obligation = Column(Numeric)
    fiscal_year_and_quarter_co = Column(Text)
    funding_agency_code = Column(Text)
    funding_agency_name = Column(Text)
    funding_office_name = Column(Text)
    funding_office_code = Column(Text)
    funding_sub_tier_agency_co = Column(Text)
    funding_sub_tier_agency_na = Column(Text)
    legal_entity_address_line1 = Column(Text)
    legal_entity_address_line2 = Column(Text)
    legal_entity_address_line3 = Column(Text)
    legal_entity_city_code = Column(Text)
    legal_entity_city_name = Column(Text)
    legal_entity_congressional = Column(Text)
    legal_entity_country_code = Column(Text)
    legal_entity_county_code = Column(Text)
    legal_entity_county_name = Column(Text)
    legal_entity_foreign_city = Column(Text)
    legal_entity_foreign_posta = Column(Text)
    legal_entity_foreign_provi = Column(Text)
    legal_entity_state_code = Column(Text)
    legal_entity_state_name = Column(Text)
    legal_entity_zip5 = Column(Text)
    legal_entity_zip_last4 = Column(Text)
    non_federal_funding_amount = Column(Numeric)
    original_loan_subsidy_cost = Column(Numeric)
    period_of_performance_curr = Column(Text)
    period_of_performance_star = Column(Text)
    place_of_performance_city = Column(Text)
    place_of_performance_code = Column(Text)
    place_of_performance_congr = Column(Text)
    place_of_perform_country_c = Column(Text)
    place_of_perform_county_na = Column(Text)
    place_of_performance_forei = Column(Text)
    place_of_perform_state_nam = Column(Text)
    place_of_performance_zip4a = Column(Text)
    record_type = Column(Integer)
    sai_number = Column(Text)
    total_funding_amount = Column(Numeric)
    uri = Column(Text, index=True)
    valid_record = Column(Boolean, nullable=False, default=True, server_default="True")
    is_first_quarter = Column(Boolean, nullable=False, default=False, server_default="False")

    def __init__(self, **kwargs):
        # broker is set up to ignore extra columns in submitted data
        # so get rid of any extraneous kwargs before instantiating
        cleanKwargs = {k: v for k, v in kwargs.items() if hasattr(self, k)}
        super(AwardFinancialAssistance, self).__init__(**cleanKwargs)
