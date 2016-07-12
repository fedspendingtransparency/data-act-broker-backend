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
    adjustmentstounobligatedbalancebroughtforward_cpe = Column(
        "adjustments_to_unobligated_cpe", Numeric)
    agencyidentifier = Column("agency_identifier", Text)
    agencyidentifier_padded = Column("agency_identifier_padded", Boolean, default=False, server_default="False")
    allocationtransferagencyidentifier = Column(
        "allocation_transfer_agency", Text)
    allocationtransferagencyidentifier_padded = Column("allocation_transfer_agency_padded", Boolean, default=False, server_default="False")
    availabilitytypecode = Column("availability_type_code", Text)
    beginningperiodofavailability = Column("beginning_period_of_availa", Text)
    borrowingauthorityamounttotal_cpe = Column(
        "borrowing_authority_amount_cpe",Numeric)
    budgetauthorityappropriatedamount_cpe = Column(
        "budget_authority_appropria_cpe", Numeric)
    budgetauthorityavailableamounttotal_cpe = Column(
        "budget_authority_available_cpe", Numeric)
    budgetauthorityunobligatedbalancebroughtforward_fyb = Column(
        "budget_authority_unobligat_fyb", Numeric)
    contractauthorityamounttotal_cpe = Column(
        "contract_authority_amount_cpe", Numeric)
    deobligationsrecoveriesrefundsbytas_cpe = Column(
        "deobligations_recoveries_r_cpe", Numeric)
    endingperiodofavailability = Column(
        "ending_period_of_availabil", Text)
    grossoutlayamountbytas_cpe = Column(
        "gross_outlay_amount_by_tas_cpe", Numeric)
    mainaccountcode = Column("main_account_code", Text)
    mainaccountcode_padded = Column("main_account_code_padded", Boolean, default=False, server_default="False")
    obligationsincurredtotalbytas_cpe = Column(
        "obligations_incurred_total_cpe", Numeric)
    otherbudgetaryresourcesamount_cpe = Column(
        "other_budgetary_resources_cpe", Numeric)
    spendingauthorityfromoffsettingcollectionsamounttotal_cpe = Column(
        "spending_authority_from_of_cpe", Numeric)
    statusofbudgetaryresourcestotal_cpe = Column(
        "status_of_budgetary_resour_cpe", Numeric)
    subaccountcode = Column("sub_account_code", Text)
    subaccountcode_padded = Column("sub_account_code_padded", Boolean, default=False, server_default="False")
    unobligatedbalance_cpe = Column("unobligated_balance_cpe", Numeric)
    tas = Column(Text, index=True, nullable=False, default=concatTas, onupdate=concatTas)
    valid_record = Column(Boolean, nullable = False, default = True, server_default = "True")
    is_first_quarter = Column(Boolean, nullable = False, default = False, server_default = "False")

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
    agencyidentifier = Column("agency_identifier", Text)
    agencyidentifier_padded = Column("agency_identifier_padded", Boolean, default=False, server_default="False")
    allocationtransferagencyidentifier = Column(
        "allocation_transfer_agency", Text)
    allocationtransferagencyidentifier_padded = Column("allocation_transfer_agency_padded", Boolean, default=False, server_default="False")
    availabilitytypecode = Column("availability_type_code", Text)
    beginningperiodofavailability = Column("beginning_period_of_availa", Text)
    bydirectreimbursablefundingsource = Column(
        "by_direct_reimbursable_fun", Text)
    deobligationsrecoveriesrefundsdofprioryearbyprogramobjectclass_cpe = Column(
        "deobligations_recov_by_pro_cpe", Numeric)
    endingperiodofavailability = Column("ending_period_of_availabil", Text)
    grossoutlayamountbyprogramobjectclass_cpe = Column(
        "gross_outlay_amount_by_pro_cpe", Numeric)
    grossoutlayamountbyprogramobjectclass_fyb = Column(
        "gross_outlay_amount_by_pro_fyb", Numeric)
    grossoutlaysdeliveredorderspaidtotal_cpe = Column(
        "gross_outlays_delivered_or_cpe", Numeric)
    grossoutlaysdeliveredorderspaidtotal_fyb = Column(
        "gross_outlays_delivered_or_fyb", Numeric)
    grossoutlaysundeliveredordersprepaidtotal_cpe = Column(
        "gross_outlays_undelivered_cpe", Numeric)
    grossoutlaysundeliveredordersprepaidtotal_fyb = Column(
        "gross_outlays_undelivered_fyb", Numeric)
    mainaccountcode = Column("main_account_code", Text)
    mainaccountcode_padded = Column("main_account_code_padded", Boolean, default=False, server_default="False")
    objectclass = Column("object_class", Text)
    obligationsdeliveredordersunpaidtotal_cpe = Column(
        "obligations_delivered_orde_cpe", Numeric)
    obligationsdeliveredordersunpaidtotal_fyb = Column(
        "obligations_delivered_orde_fyb", Numeric)
    obligationsincurredbyprogramobjectclass_cpe = Column(
        "obligations_incurred_by_pr_cpe", Numeric)
    obligationsundeliveredordersunpaidtotal_cpe = Column(
        "obligations_undelivered_or_cpe", Numeric)
    obligationsundeliveredordersunpaidtotal_fyb = Column(
        "obligations_undelivered_or_fyb", Numeric)
    programactivitycode = Column("program_activity_code", Text)
    programactivitycode_padded = Column("program_activity_code_padded", Boolean, default=False, server_default="False")
    programactivityname = Column("program_activity_name", Text)
    subaccountcode = Column("sub_account_code", Text)
    subaccountcode_padded = Column("sub_account_code_padded", Boolean, default=False, server_default="False")
    ussgl480100_undeliveredordersobligationsunpaid_cpe = Column(
        "ussgl480100_undelivered_or_cpe", Numeric)
    ussgl480100_undeliveredordersobligationsunpaid_fyb = Column(
        "ussgl480100_undelivered_or_fyb", Numeric)
    ussgl480200_undeliveredordersobligationsprepaidadvanced_cpe = Column(
        "ussgl480200_undelivered_or_cpe", Numeric)
    ussgl480200_undeliveredordersobligationsprepaidadvanced_fyb = Column(
        "ussgl480200_undelivered_or_fyb", Numeric)
    ussgl483100_undeliveredordersobligationstransferredunpaid_cpe = Column(
        "ussgl483100_undelivered_or_cpe", Numeric)
    ussgl483200_undeliveredordersobligationstransferredprepaidadvanced_cpe = Column(
        "ussgl483200_undelivered_or_cpe", Numeric)
    ussgl487100_downwardadjustmentsofprioryearunpaidundeliveredordersobligationsrecoveries_cpe = Column(
        "ussgl487100_downward_adjus_cpe", Numeric)
    ussgl487200_downwardadjustmentsofprioryearprepaidadvancedundeliveredordersobligationsrefundscollected_cpe = Column(
        "ussgl487200_downward_adjus_cpe", Numeric)
    ussgl488100_upwardadjustmentsofprioryearundeliveredordersobligationsunpaid_cpe = Column(
        "ussgl488100_upward_adjustm_cpe", Numeric)
    ussgl488200_upwardadjustmentsofprioryearundeliveredordersobligationsprepaidadvanced_cpe = Column(
        "ussgl488200_upward_adjustm_cpe", Numeric)
    ussgl490100_deliveredordersobligationsunpaid_cpe = Column(
        "ussgl490100_delivered_orde_cpe", Numeric)
    ussgl490100_deliveredordersobligationsunpaid_fyb = Column(
        "ussgl490100_delivered_orde_fyb", Numeric)
    ussgl490200_deliveredordersobligationspaid_cpe = Column(
        "ussgl490200_delivered_orde_cpe", Numeric)
    ussgl490800_authorityoutlayednotyetdisbursed_cpe = Column(
        "ussgl490800_authority_outl_cpe", Numeric)
    ussgl490800_authorityoutlayednotyetdisbursed_fyb = Column(
        "ussgl490800_authority_outl_fyb", Numeric)
    ussgl493100_deliveredordersobligationstransferredunpaid_cpe = Column(
        "ussgl493100_delivered_orde_cpe", Numeric)
    ussgl497100_downwardadjustmentsofprioryearunpaiddeliveredordersobligationsrecoveries_cpe = Column(
        "ussgl497100_downward_adjus_cpe", Numeric)
    ussgl497200_downwardadjustmentsofprioryearpaiddeliveredordersobligationsrefundscollected_cpe = Column(
        "ussgl497200_downward_adjus_cpe", Numeric)
    ussgl498100_upwardadjustmentsofprioryeardeliveredordersobligationsunpaid_cpe = Column(
        "ussgl498100_upward_adjustm_cpe", Numeric)
    ussgl498200_upwardadjustmentsofprioryeardeliveredordersobligationspaid_cpe = Column(
        "ussgl498200_upward_adjustm_cpe", Numeric)
    tas = Column(Text, nullable=False, default=concatTas, onupdate=concatTas)
    valid_record = Column(Boolean, nullable = False, default = True, server_default = "True")
    is_first_quarter = Column(Boolean, nullable = False, default = False, server_default = "False")

    def __init__(self, **kwargs):
        # broker is set up to ignore extra columns in submitted data
        # so get rid of any extraneous kwargs before instantiating
        cleanKwargs = {k: v for k, v in kwargs.items() if hasattr(self, k)}
        super(ObjectClassProgramActivity, self).__init__(**cleanKwargs)

Index("ix_oc_pa_tas_oc_pa",
      ObjectClassProgramActivity.tas,
      ObjectClassProgramActivity.objectclass,
      ObjectClassProgramActivity.programactivitycode,
      unique=False)

class AwardFinancial(Base):
    """Model for the award_financial table."""
    __tablename__ = "award_financial"

    award_financial_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, nullable=False, index=True)
    job_id = Column(Integer, nullable=False, index=True)
    row_number = Column(Integer, nullable=False)
    agencyidentifier = Column("agency_identifier", Text)
    agencyidentifier_padded = Column("agency_identifier_padded", Boolean, default=False, server_default="False")
    allocationtransferagencyidentifier = Column(
        "allocation_transfer_agency", Text)
    allocationtransferagencyidentifier_padded = Column("allocation_transfer_agency_padded", Boolean, default=False, server_default="False")
    availabilitytypecode = Column("availability_type_code", Text)
    beginningperiodofavailability = Column("beginning_period_of_availa", Text)
    bydirectreimbursablefundingsource = Column(
        "by_direct_reimbursable_fun", Text)
    deobligationsrecoveriesrefundsofprioryearbyaward_cpe = Column(
        "deobligations_recov_by_awa_cpe", Numeric)
    endingperiodofavailability = Column("ending_period_of_availabil", Text)
    fain = Column(Text, index=True)
    grossoutlayamountbyaward_cpe = Column(
        "gross_outlay_amount_by_awa_cpe", Numeric)
    grossoutlayamountbyaward_fyb = Column(
        "gross_outlay_amount_by_awa_fyb", Numeric)
    grossoutlaysdeliveredorderspaidtotal_cpe = Column(
        "gross_outlays_delivered_or_cpe", Numeric)
    grossoutlaysdeliveredorderspaidtotal_fyb = Column(
        "gross_outlays_delivered_or_fyb", Numeric)
    grossoutlaysundeliveredordersprepaidtotal_cpe = Column(
        "gross_outlays_undelivered_cpe", Numeric)
    grossoutlaysundeliveredordersprepaidtotal_fyb = Column(
        "gross_outlays_undelivered_fyb", Numeric)
    mainaccountcode = Column("main_account_code", Text)
    mainaccountcode_padded = Column("main_account_code_padded", Boolean, default=False, server_default="False")
    objectclass = Column("object_class", Text)
    obligationsdeliveredordersunpaidtotal_cpe = Column(
        "obligations_delivered_orde_cpe", Numeric)
    obligationsdeliveredordersunpaidtotal_fyb = Column(
        "obligations_delivered_orde_fyb", Numeric)
    obligationsincurredtotalbyaward_cpe = Column(
        "obligations_incurred_byawa_cpe", Numeric)
    obligationsundeliveredordersunpaidtotal_cpe = Column(
        "obligations_undelivered_or_cpe", Numeric)
    obligationsundeliveredordersunpaidtotal_fyb = Column(
        "obligations_undelivered_or_fyb", Numeric)
    parentawardid = Column("parent_award_id", Text)
    piid = Column(Text, index=True)
    programactivitycode = Column("program_activity_code", Text)
    programactivitycode_padded = Column("program_activity_code_padded", Boolean, default=False, server_default="False")
    programactivityname = Column("program_activity_name", Text)
    subaccountcode = Column("sub_account_code", Text)
    subaccountcode_padded = Column("sub_account_code_padded", Boolean, default=False, server_default="False")
    transactionobligatedamount = Column("transaction_obligated_amou", Numeric)
    uri = Column(Text, index=True)
    ussgl480100_undeliveredordersobligationsunpaid_cpe = Column(
        "ussgl480100_undelivered_or_cpe", Numeric)
    ussgl480100_undeliveredordersobligationsunpaid_fyb = Column(
        "ussgl480100_undelivered_or_fyb", Numeric)
    ussgl480200_undeliveredordersobligationsprepaidadvanced_cpe = Column(
        "ussgl480200_undelivered_or_cpe", Numeric)
    ussgl480200_undeliveredordersobligationsprepaidadvanced_fyb = Column(
        "ussgl480200_undelivered_or_fyb", Numeric)
    ussgl483100_undeliveredordersobligationstransferredunpaid_cpe = Column(
        "ussgl483100_undelivered_or_cpe", Numeric)
    ussgl483200_undeliveredordersobligationstransferredprepaidadvanced_cpe = Column(
        "ussgl483200_undelivered_or_cpe", Numeric)
    ussgl487100_downwardadjustmentsofprioryearunpaidundeliveredordersobligationsrecoveries_cpe = Column(
        "ussgl487100_downward_adjus_cpe", Numeric)
    ussgl487200_downwardadjustmentsofprioryearprepaidadvancedundeliveredordersobligationsrefundscollected_cpe = Column(
        "ussgl487200_downward_adjus_cpe", Numeric)
    ussgl488100_upwardadjustmentsofprioryearundeliveredordersobligationsunpaid_cpe = Column(
        "ussgl488100_upward_adjustm_cpe", Numeric)
    ussgl488200_upwardadjustmentsofprioryearundeliveredordersobligationsprepaidadvanced_cpe = Column(
        "ussgl488200_upward_adjustm_cpe", Numeric)
    ussgl490100_deliveredordersobligationsunpaid_cpe = Column(
        "ussgl490100_delivered_orde_cpe", Numeric)
    ussgl490100_deliveredordersobligationsunpaid_fyb = Column(
        "ussgl490100_delivered_orde_fyb", Numeric)
    ussgl490200_deliveredordersobligationspaid_cpe = Column(
        "ussgl490200_delivered_orde_cpe", Numeric)
    ussgl490800_authorityoutlayednotyetdisbursed_cpe = Column(
        "ussgl490800_authority_outl_cpe", Numeric)
    ussgl490800_authorityoutlayednotyetdisbursed_fyb = Column(
        "ussgl490800_authority_outl_fyb", Numeric)
    ussgl493100_deliveredordersobligationstransferredunpaid_cpe = Column(
        "ussgl493100_delivered_orde_cpe", Numeric)
    ussgl497100_downwardadjustmentsofprioryearunpaiddeliveredordersobligationsrecoveries_cpe = Column(
        "ussgl497100_downward_adjus_cpe", Numeric)
    ussgl497200_downwardadjustmentsofprioryearpaiddeliveredordersobligationsrefundscollected_cpe = Column(
        "ussgl497200_downward_adjus_cpe", Numeric)
    ussgl498100_upwardadjustmentsofprioryeardeliveredordersobligationsunpaid_cpe  = Column(
        "ussgl498100_upward_adjustm_cpe", Numeric)
    ussgl498200_upwardadjustmentsofprioryeardeliveredordersobligationspaid_cpe = Column(
        "ussgl498200_upward_adjustm_cpe", Numeric)
    tas = Column(Text, nullable=False, default=concatTas, onupdate=concatTas)
    valid_record = Column(Boolean, nullable = False, default = True, server_default = "True")
    is_first_quarter = Column(Boolean, nullable = False, default = False, server_default = "False")

    def __init__(self, **kwargs):
        # broker is set up to ignore extra columns in submitted data
        # so get rid of any extraneous kwargs before instantiating
        cleanKwargs = {k: v for k, v in kwargs.items() if hasattr(self, k)}
        super(AwardFinancial, self).__init__(**cleanKwargs)

Index("ix_award_financial_tas_oc_pa",
      AwardFinancial.tas,
      AwardFinancial.objectclass,
      AwardFinancial.programactivitycode,
      unique=False)

class AwardFinancialAssistance(Base):
    """Model for the award_financial_assistance table."""
    __tablename__ = "award_financial_assistance"

    award_financial_assistance_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, nullable=False, index=True)
    job_id = Column(Integer, nullable=False, index=True)
    row_number = Column(Integer, nullable=False)
    actiondate = Column("action_date", Text)
    actiontype = Column("action_type", Text)
    assistancetype = Column("assistance_type", Text)
    awarddescription = Column("award_description", Text)
    awardeeorrecipientlegalentityname = Column(
        "awardee_or_recipient_legal", Text)
    awardeeorrecipientuniqueidentifier = Column(
        "awardee_or_recipient_uniqu", Text)
    awardingagencycode = Column("awarding_agency_code", Text)
    awardingagencyname = Column("awarding_agency_name", Text)
    awardingofficecode = Column("awarding_office_code", Text)
    awardingofficename = Column("awarding_office_name", Text)
    awardingsubtieragencycode = Column("awarding_sub_tier_agency_c", Text)
    awardingsubtieragencyname = Column("awarding_sub_tier_agency_n", Text)
    awardmodificationamendmentnumber = Column(
        "award_modification_amendme", Text)
    businessfundsindicator = Column("business_funds_indicator", Text)
    businesstypes = Column("business_types", Text)
    cfda_number = Column("cfda_number", Text)
    cfda_title = Column("cfda_title", Text)
    correctionlatedeleteindicator = Column("correction_late_delete_ind", Text)
    facevalueloanguarantee = Column("face_value_loan_guarantee", Numeric)
    fain = Column(Text, index=True)
    federalactionobligation = Column("federal_action_obligation", Numeric)
    fiscalyearandquartercorrection = Column("fiscal_year_and_quarter_co", Text)
    fundingagencycode = Column("funding_agency_code", Text)
    fundingagencyname = Column("funding_agency_name", Text)
    fundingagencyofficename = Column("funding_office_name", Text)
    fundingofficecode = Column("funding_office_code", Text)
    fundingsubtieragencycode = Column("funding_sub_tier_agency_co", Text)
    fundingsubtieragencyname = Column("funding_sub_tier_agency_na", Text)
    legalentityaddressline1 = Column("legal_entity_address_line1", Text)
    legalentityaddressline2 = Column("legal_entity_address_line2", Text)
    legalentityaddressline3 = Column("legal_entity_address_line3", Text)
    legalentitycitycode = Column("legal_entity_city_code", Text)
    legalentitycityname = Column("legal_entity_city_name", Text)
    legalentitycongressionaldistrict = Column("legal_entity_congressional", Text)
    legalentitycountrycode = Column("legal_entity_country_code", Text)
    legalentitycountycode = Column("legal_entity_county_code", Text)
    legalentitycountyname = Column("legal_entity_county_name", Text)
    legalentityforeigncityname = Column("legal_entity_foreign_city", Text)
    legalentityforeignpostalcode = Column("legal_entity_foreign_posta", Text)
    legalentityforeignprovincename = Column("legal_entity_foreign_provi", Text)
    legalentitystatecode = Column("legal_entity_state_code", Text)
    legalentitystatename = Column("legal_entity_state_name", Text)
    legalentityzip5 = Column("legal_entity_zip5", Text)
    legalentityziplast4 = Column("legal_entity_zip_last4", Text)
    nonfederalfundingamount = Column("non_federal_funding_amount", Numeric)
    originalloansubsidycost = Column("original_loan_subsidy_cost", Numeric)
    periodofperformancecurrentenddate = Column(
        "period_of_performance_curr", Text)
    periodofperformancestartdate = Column("period_of_performance_star", Text)
    primaryplaceofperformancecityname = Column(
        "place_of_performance_city", Text)
    primaryplaceofperformancecode = Column("place_of_performance_code", Text)
    primaryplaceofperformancecongressionaldistrict = Column(
        "place_of_performance_congr", Text)
    primaryplaceofperformancecountrycode = Column(
        "place_of_perform_country_c", Text)
    primaryplaceofperformancecountyname = Column(
        "place_of_perform_county_na", Text)
    primaryplaceofperformanceforeignlocationdescription = Column(
        "place_of_performance_forei", Text)
    primaryplaceofperformancestatename = Column(
        "place_of_perform_state_nam", Text)
    primaryplaceofperformancezipplus4 = Column(
        "place_of_performance_zip4a", Text)
    recordtype = Column("record_type", Integer)
    sai_number = Column("sai_number", Text)
    totalfundingamount = Column("total_funding_amount", Numeric)
    uri = Column(Text, index=True)
    valid_record = Column(Boolean, nullable = False, default = True, server_default = "True")
    is_first_quarter = Column(Boolean, nullable = False, default = False, server_default = "False")

    def __init__(self, **kwargs):
        # broker is set up to ignore extra columns in submitted data
        # so get rid of any extraneous kwargs before instantiating
        cleanKwargs = {k: v for k, v in kwargs.items() if hasattr(self, k)}
        super(AwardFinancialAssistance, self).__init__(**cleanKwargs)
