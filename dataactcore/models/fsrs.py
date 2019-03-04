from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, func, Integer, String, Text
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
        """We'll often want to load "new" data -- anything with a later id
        than the awards we have. Return that max id"""
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


class FileF(Base):
    __tablename__ = "file_f"
    internal_id = Column(String)
    PrimeAwardUniqueKey = Column(String)
    PrimeAwardID = Column(String)
    ParentAwardID = Column(String)
    PrimeAwardAmount = Column(String)
    ActionDate = Column(String)
    PrimeAwardFiscalYear = Column(String)
    AwardingAgencyCode = Column(String)
    AwardingAgencyName = Column(String)
    AwardingSubTierAgencyCode = Column(String)
    AwardingSubTierAgencyName = Column(String)
    AwardingOfficeCode = Column(String)
    AwardingOfficeName = Column(String)
    FundingAgencyCode = Column(String)
    FundingAgencyName = Column(String)
    FundingSubTierAgencyCode = Column(String)
    FundingSubTierAgencyName = Column(String)
    FundingOfficeCode = Column(String)
    FundingOfficeName = Column(String)
    AwardeeOrRecipientUniqueIdentifier = Column(String)
    AwardeeOrRecipientLegalEntityName = Column(String)
    VendoDoingAsBusinessName = Column(String)
    UltimateParentUniqueIdentifier = Column(String)
    UltimateParentLegalEntityName = Column(String)
    LegalEntityCountryCode = Column(String)
    LegalEntityCountryName = Column(String)
    LegalEntityAddressLine1 = Column(String)
    LegalEntityCityName = Column(String)
    LegalEntityStateCode = Column(String)
    LegalEntityStateName = Column(String)
    LegalEntityZIP_4 = Column(String)
    LegalEntityCongressionalDistrict = Column(String)
    LegalEntityForeignPostalCode = Column(String)
    PrimeAwardeeBusinessTypes = Column(String)
    PrimaryPlaceOfPerformanceCityName = Column(String)
    PrimaryPlaceOfPerformanceStateCode = Column(String)
    PrimaryPlaceOfPerformanceStateName = Column(String)
    PrimaryPlaceOfPerformanceZIP_4 = Column(String)
    PrimaryPlaceOfPerformanceCongressionalDistrict = Column(String)
    PrimaryPlaceOfPerformanceCountryCode = Column(String)
    PrimaryPlaceOfPerformanceCountryName = Column(String)
    AwardDescription = Column(String)
    NAICS = Column(String)
    NAICS_Description = Column(String)
    CFDA_Numbers = Column(String)
    CFDA_Titles = Column(String)
    SubAwardType = Column(String)
    SubAwardReportYear = Column(String)
    SubAwardReportMonth = Column(String)
    SubAwardNumber = Column(String)
    SubAwardAmount = Column(String)
    SubAwardActionDate = Column(String)
    SubAwardeeOrRecipientUniqueIdentifier = Column(String)
    SubAwardeeOrRecipientLegalEntityName = Column(String)
    SubAwardeeDoingBusinessAsName = Column(String)
    SubAwardeeUltimateParentUniqueIdentifier = Column(String)
    SubAwardeeUltimateParentLegalEntityName = Column(String)
    SubAwardeeLegalEntityCountryCode = Column(String)
    SubAwardeeLegalEntityCountryName = Column(String)
    SubAwardeeLegalEntityAddressLine1 = Column(String)
    SubAwardeeLegalEntityCityName = Column(String)
    SubAwardeeLegalEntityStateCode = Column(String)
    SubAwardeeLegalEntityStateName = Column(String)
    SubAwardeeLegalEntityZIP_4 = Column(String)
    SubAwardeeLegalEntityCongressionalDistrict = Column(String)
    SubAwardeeLegalEntityForeignPostalCode = Column(String)
    SubAwardeeBusinessTypes = Column(String)
    SubAwardPlaceOfPerformanceCityName = Column(String)
    SubAwardPlaceOfPerformanceStateCode = Column(String)
    SubAwardPlaceOfPerformanceStateName = Column(String)
    SubAwardPlaceOfPerformanceZIP_4 = Column(String)
    SubAwardPlaceOfPerformanceCongressionalDistrict = Column(String)
    SubAwardPlaceOfPerformanceCountryCode = Column(String)
    SubAwardPlaceOfPerformanceCountryName = Column(String)
    SubAwardDescription = Column(String)
    SubAwardeeHighCompOfficer1FullName = Column(String)
    SubAwardeeHighCompOfficer1Amount = Column(String)
    SubAwardeeHighCompOfficer2FullName = Column(String)
    SubAwardeeHighCompOfficer2Amount = Column(String)
    SubAwardeeHighCompOfficer3FullName = Column(String)
    SubAwardeeHighCompOfficer3Amount = Column(String)
    SubAwardeeHighCompOfficer4FullName = Column(String)
    SubAwardeeHighCompOfficer4Amount = Column(String)
    SubAwardeeHighCompOfficer5FullName = Column(String)
    SubAwardeeHighCompOfficer5Amount = Column(String)
