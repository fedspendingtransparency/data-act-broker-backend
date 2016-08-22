from sqlalchemy import (
    Boolean, Column, Date, DateTime, ForeignKey, Integer, String, Text)
from sqlalchemy.orm import relationship

from dataactcore.models.baseModel import Base


class CommonAttributes():
    duns = Column(String)
    company_name = Column(String)
    dba_name = Column(String)
    bus_types = Column(String)
    company_address_city = Column(String)
    company_address_street = Column(String, nullable=True)
    company_address_state = Column(String)
    company_address_country = Column(String)
    company_address_zip = Column(String)
    company_address_district = Column(String, nullable=True)
    principle_place_city = Column(String)
    principle_place_street = Column(String, nullable=True)
    principle_place_state = Column(String)
    principle_place_country = Column(String)
    principle_place_zip = Column(String)
    principle_place_district = Column(String, nullable=True)
    parent_duns = Column(String)
    parent_company_name = Column(String)
    naics = Column(String)
    funding_agency_id = Column(String)
    funding_agency_name = Column(String)
    funding_office_id = Column(String)
    funding_office_name = Column(String)
    recovery_model_q1 = Column(Boolean)
    recovery_model_q2 = Column(Boolean)
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


class FSRSAward(Base, CommonAttributes):
    __tablename__ = "fsrs_award"
    id = Column(Integer, primary_key=True)
    internal_id = Column(String)
    contract_number = Column(String)
    idv_reference_number = Column(String, nullable=True)
    date_submitted = Column(DateTime)
    report_period_mon = Column(String)
    report_period_year = Column(String)
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


class FSRSSubaward(Base, CommonAttributes):
    __tablename__ = "fsrs_subaward"
    id = Column(Integer, primary_key=True)
    award_id = Column(
        Integer, ForeignKey('fsrs_award.id', ondelete='CASCADE'))
    award = relationship(FSRSAward, back_populates='subawards')
    subcontract_amount = Column(String)
    subcontract_date = Column(Date)
    subcontract_num = Column(String)
    overall_description = Column(Text)
    recovery_subcontract_amt = Column(String, nullable=True)

FSRSAward.subawards = relationship(FSRSSubaward, back_populates='award')
