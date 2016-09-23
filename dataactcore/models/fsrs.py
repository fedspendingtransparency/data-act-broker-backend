from sqlalchemy import (
    Boolean, Column, Date, DateTime, ForeignKey, func, Integer, String, Text)
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
    awardee_address_country = Column(String)
    awardee_address_zip = Column(String)
    awardee_address_district = Column(String, nullable=True)
    cfda_numbers = Column(String)
    project_description = Column(String)
    compensation_q1 = Column(Boolean)
    compensation_q2 = Column(Boolean)


class _PrimeAwardAttributes:
    """Attributes shared by FSRSProcurements and FSRSGrants"""
    internal_id = Column(String)
    date_submitted = Column(DateTime)
    report_period_mon = Column(String)
    report_period_year = Column(String)

    @classmethod
    def nextId(cls, sess):
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
    parent_id = Column(
        Integer, ForeignKey('fsrs_procurement.id', ondelete='CASCADE'))
    parent = relationship(FSRSProcurement, back_populates='subawards')
    subcontract_amount = Column(String)
    subcontract_date = Column(Date)
    subcontract_num = Column(String)
    overall_description = Column(Text)
    recovery_subcontract_amt = Column(String, nullable=True)

FSRSProcurement.subawards = relationship(
    FSRSSubcontract, back_populates='parent')


class FSRSGrant(Base, _GrantAttributes, _PrimeAwardAttributes):
    __tablename__ = "fsrs_grant"
    fain = Column(String, index=True)
    total_fed_funding_amount = Column(String)
    obligation_date = Column(Date)


class FSRSSubgrant(Base, _GrantAttributes):
    __tablename__ = "fsrs_subgrant"
    parent_id = Column(
        Integer, ForeignKey('fsrs_grant.id', ondelete='CASCADE'))
    parent = relationship(FSRSGrant, back_populates='subawards')
    subaward_amount = Column(String)
    subaward_date = Column(Date)
    subaward_num = Column(String)

FSRSGrant.subawards = relationship(FSRSSubgrant, back_populates='parent')
