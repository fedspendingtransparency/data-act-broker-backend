from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, func, Integer, String, Text, Index
from sqlalchemy.orm import relationship

from dataactcore.models.baseModel import Base


class _FSRSAttributes:
    """Attributes shared by all FSRS models"""
    id = Column(Integer, primary_key=True)
    duns = Column(String)
    dba_name = Column(String)
    principle_place_city = Column(String)
    principle_place_street = Column(String, nullable=True)
    principle_place_state = Column(String)
    principle_place_state_name = Column(String)
    principle_place_country = Column(String)
    principle_place_zip = Column(String)
    principle_place_district = Column(String, nullable=True)
    parent_duns = Column(String)
    funding_agency_id = Column(String)
    funding_agency_name = Column(String)
    top_paid_fullname_1 = Column(String, nullable=True)
    top_paid_amount_1 = Column(String, nullable=True)
    top_paid_fullname_2 = Column(String, nullable=True)
    top_paid_amount_2 = Column(String, nullable=True)
    top_paid_fullname_3 = Column(String, nullable=True)
    top_paid_amount_3 = Column(String, nullable=True)
    top_paid_fullname_4 = Column(String, nullable=True)
    top_paid_amount_4 = Column(String, nullable=True)
    top_paid_fullname_5 = Column(String, nullable=True)
    top_paid_amount_5 = Column(String, nullable=True)


class _ContractAttributes(_FSRSAttributes):
    """Common attributes of FSRSProcurement and FSRSSubcontracts"""
    company_name = Column(String)
    bus_types = Column(String)
    company_address_city = Column(String)
    company_address_street = Column(String, nullable=True)
    company_address_state = Column(String)
    company_address_state_name = Column(String)
    company_address_country = Column(String)
    company_address_zip = Column(String)
    company_address_district = Column(String, nullable=True)
    parent_company_name = Column(String)
    naics = Column(String)
    funding_office_id = Column(String)
    funding_office_name = Column(String)
    recovery_model_q1 = Column(Boolean)
    recovery_model_q2 = Column(Boolean)


class _GrantAttributes(_FSRSAttributes):
    """Common attributes of FSRSGrant and FSRSSubgrant"""
    dunsplus4 = Column(String, nullable=True)
    awardee_name = Column(String)
    awardee_address_city = Column(String)
    awardee_address_street = Column(String, nullable=True)
    awardee_address_state = Column(String)
    awardee_address_state_name = Column(String)
    awardee_address_country = Column(String)
    awardee_address_zip = Column(String)
    awardee_address_district = Column(String, nullable=True)
    cfda_numbers = Column(String)
    project_description = Column(String)
    compensation_q1 = Column(Boolean)
    compensation_q2 = Column(Boolean)
    federal_agency_id = Column(String)
    federal_agency_name = Column(String)


class _PrimeAwardAttributes:
    """Attributes shared by FSRSProcurements and FSRSGrants"""
    internal_id = Column(String)
    date_submitted = Column(DateTime)
    report_period_mon = Column(String)
    report_period_year = Column(String)

    @classmethod
    def next_id(cls, sess):
        # We'll often want to load "new" data -- anything with a later id than the awards we have. Return that max id.
        current = sess.query(func.max(cls.id)).one()[0] or -1
        return current + 1


class FSRSProcurement(Base, _ContractAttributes, _PrimeAwardAttributes):
    __tablename__ = "fsrs_procurement"
    contract_number = Column(String, index=True)
    idv_reference_number = Column(String, nullable=True)
    report_type = Column(String)
    contract_agency_code = Column(String)
    contract_idv_agency_code = Column(String, nullable=True)
    contracting_office_aid = Column(String)
    contracting_office_aname = Column(String)
    contracting_office_id = Column(String)
    contracting_office_name = Column(String)
    treasury_symbol = Column(String)
    dollar_obligated = Column(String)
    date_signed = Column(Date)
    transaction_type = Column(String)
    program_title = Column(String)


class FSRSSubcontract(Base, _ContractAttributes):
    __tablename__ = "fsrs_subcontract"
    parent_id = Column(Integer, ForeignKey('fsrs_procurement.id', ondelete='CASCADE'), index=True)
    parent = relationship(FSRSProcurement, back_populates='subawards')
    subcontract_amount = Column(String)
    subcontract_date = Column(Date)
    subcontract_num = Column(String)
    overall_description = Column(Text)
    recovery_subcontract_amt = Column(String, nullable=True)

FSRSProcurement.subawards = relationship(FSRSSubcontract, back_populates='parent')


class FSRSGrant(Base, _GrantAttributes, _PrimeAwardAttributes):
    __tablename__ = "fsrs_grant"
    fain = Column(String, index=True)
    total_fed_funding_amount = Column(String)
    obligation_date = Column(Date)


class FSRSSubgrant(Base, _GrantAttributes):
    __tablename__ = "fsrs_subgrant"
    parent_id = Column(Integer, ForeignKey('fsrs_grant.id', ondelete='CASCADE'), index=True)
    parent = relationship(FSRSGrant, back_populates='subawards')
    subaward_amount = Column(String)
    subaward_date = Column(Date)
    subaward_num = Column(String)

FSRSGrant.subawards = relationship(FSRSSubgrant, back_populates='parent')

Index("ix_fsrs_proc_contract_number_upper", func.upper(FSRSProcurement.contract_number))
Index("ix_fsrs_proc_idv_ref_upper", func.upper(FSRSProcurement.idv_reference_number))
Index("ix_fsrs_proc_contract_office_aid_upper", func.upper(FSRSProcurement.contracting_office_aid))
Index("ix_fsrs_grant_fain_upper", func.upper(FSRSGrant.fain))
Index("ix_fsrs_grant_federal_agency_id_upper", func.upper(FSRSGrant.federal_agency_id))


class Subaward(Base):
    """ Model for all subaward data """
    __tablename__ = "subaward"
    id = Column(Integer, primary_key=True)

    # File F - Prime Award Data
    unique_award_key = Column(Text, index=True)
    award_id = Column(Text, index=True)
    parent_award_id = Column(Text, index=True)
    award_amount = Column(Text)
    action_date = Column(Text, index=True)
    fy = Column(Text)
    awarding_agency_code = Column(Text, index=True)
    awarding_agency_name = Column(Text)
    awarding_sub_tier_agency_c = Column(Text, index=True)
    awarding_sub_tier_agency_n = Column(Text)
    awarding_office_code = Column(Text)
    awarding_office_name = Column(Text)
    funding_agency_code = Column(Text, index=True)
    funding_agency_name = Column(Text)
    funding_sub_tier_agency_co = Column(Text, index=True)
    funding_sub_tier_agency_na = Column(Text)
    funding_office_code = Column(Text)
    funding_office_name = Column(Text)
    awardee_or_recipient_uniqu = Column(Text, index=True)
    awardee_or_recipient_legal = Column(Text)
    dba_name = Column(Text)
    ultimate_parent_unique_ide = Column(Text)
    ultimate_parent_legal_enti = Column(Text)
    legal_entity_country_code = Column(Text)
    legal_entity_country_name = Column(Text)
    legal_entity_address_line1 = Column(Text)
    legal_entity_city_name = Column(Text)
    legal_entity_state_code = Column(Text)
    legal_entity_state_name = Column(Text)
    legal_entity_zip = Column(Text)
    legal_entity_congressional = Column(Text)
    legal_entity_foreign_posta = Column(Text)
    business_types = Column(Text)
    place_of_perform_city_name = Column(Text)
    place_of_perform_state_code = Column(Text)
    place_of_perform_state_name = Column(Text)
    place_of_performance_zip = Column(Text)
    place_of_perform_congressio = Column(Text)
    place_of_perform_country_co = Column(Text)
    place_of_perform_country_na = Column(Text)
    award_description = Column(Text)
    naics = Column(Text)
    naics_description = Column(Text)
    cfda_numbers = Column(Text)
    cfda_titles = Column(Text)
    # File F - Subaward Data
    subaward_type = Column(Text, index=True)
    subaward_report_year = Column(Text)
    subaward_report_month = Column(Text)
    subaward_number = Column(Text, index=True)
    subaward_amount = Column(Text)
    sub_action_date = Column(Text, index=True)
    sub_awardee_or_recipient_uniqu = Column(Text, index=True)
    sub_awardee_or_recipient_legal = Column(Text)
    sub_dba_name = Column(Text)
    sub_ultimate_parent_unique_ide = Column(Text)
    sub_ultimate_parent_legal_enti = Column(Text)
    sub_legal_entity_country_code = Column(Text)
    sub_legal_entity_country_name = Column(Text)
    sub_legal_entity_address_line1 = Column(Text)
    sub_legal_entity_city_name = Column(Text)
    sub_legal_entity_state_code = Column(Text)
    sub_legal_entity_state_name = Column(Text)
    sub_legal_entity_zip = Column(Text)
    sub_legal_entity_congressional = Column(Text)
    sub_legal_entity_foreign_posta = Column(Text)
    sub_business_types = Column(Text)
    sub_place_of_perform_city_name = Column(Text)
    sub_place_of_perform_state_code = Column(Text)
    sub_place_of_perform_state_name = Column(Text)
    sub_place_of_performance_zip = Column(Text)
    sub_place_of_perform_congressio = Column(Text)
    sub_place_of_perform_country_co = Column(Text)
    sub_place_of_perform_country_na = Column(Text)
    subaward_description = Column(Text)
    sub_high_comp_officer1_full_na = Column(Text, nullable=True)
    sub_high_comp_officer1_amount = Column(Text, nullable=True)
    sub_high_comp_officer2_full_na = Column(Text, nullable=True)
    sub_high_comp_officer2_amount = Column(Text, nullable=True)
    sub_high_comp_officer3_full_na = Column(Text, nullable=True)
    sub_high_comp_officer3_amount = Column(Text, nullable=True)
    sub_high_comp_officer4_full_na = Column(Text, nullable=True)
    sub_high_comp_officer4_amount = Column(Text, nullable=True)
    sub_high_comp_officer5_full_na = Column(Text, nullable=True)
    sub_high_comp_officer5_amount = Column(Text, nullable=True)
    # Additional FSRS - Prime Award Data
    prime_id = Column(Integer, index=True)
    internal_id = Column(Text, index=True)
    date_submitted = Column(Text)
    report_type = Column(Text)
    transaction_type = Column(Text)
    program_title = Column(Text)
    contract_agency_code = Column(Text)
    contract_idv_agency_code = Column(Text)
    grant_funding_agency_id = Column(Text)
    grant_funding_agency_name = Column(Text)
    federal_agency_name = Column(Text)
    treasury_symbol = Column(Text)
    dunsplus4 = Column(Text)
    recovery_model_q1 = Column(Text)
    recovery_model_q2 = Column(Text)
    compensation_q1 = Column(Text)
    compensation_q2 = Column(Text)
    high_comp_officer1_full_na = Column(Text, nullable=True)
    high_comp_officer1_amount = Column(Text, nullable=True)
    high_comp_officer2_full_na = Column(Text, nullable=True)
    high_comp_officer2_amount = Column(Text, nullable=True)
    high_comp_officer3_full_na = Column(Text, nullable=True)
    high_comp_officer3_amount = Column(Text, nullable=True)
    high_comp_officer4_full_na = Column(Text, nullable=True)
    high_comp_officer4_amount = Column(Text, nullable=True)
    high_comp_officer5_full_na = Column(Text, nullable=True)
    high_comp_officer5_amount = Column(Text, nullable=True)
    place_of_perform_street = Column(Text)

    # Additional FSRS - Subaward Data
    sub_id = Column(Integer, index=True)
    sub_parent_id = Column(Integer, index=True)
    sub_federal_agency_id = Column(Text)
    sub_federal_agency_name = Column(Text)
    sub_funding_agency_id = Column(Text)
    sub_funding_agency_name = Column(Text)
    sub_funding_office_id = Column(Text)
    sub_funding_office_name = Column(Text)
    sub_naics = Column(Text)
    sub_cfda_numbers = Column(Text)
    sub_dunsplus4 = Column(Text)
    sub_recovery_subcontract_amt = Column(Text)
    sub_recovery_model_q1 = Column(Text)
    sub_recovery_model_q2 = Column(Text)
    sub_compensation_q1 = Column(Text)
    sub_compensation_q2 = Column(Text)
    sub_place_of_perform_street = Column(Text)

Index("ix_subaward_award_id_upper", func.upper(Subaward.award_id))
Index("ix_subaward_parent_award_id_upper", func.upper(Subaward.parent_award_id))
Index("ix_subaward_awarding_sub_tier_agency_c_upper", func.upper(Subaward.awarding_sub_tier_agency_c))
