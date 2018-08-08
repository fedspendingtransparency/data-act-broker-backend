from datetime import timedelta

import sqlalchemy as sa

from sqlalchemy import (Column, Date, DateTime, ForeignKey, Index, Integer, Numeric, Text, Float, UniqueConstraint,
                        Boolean, ARRAY)
from sqlalchemy.orm import relationship
from dataactcore.models.baseModel import Base


def concat_tas(context):
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


TAS_COMPONENTS = (
    'allocation_transfer_agency', 'agency_identifier', 'beginning_period_of_availa', 'ending_period_of_availabil',
    'availability_type_code', 'main_account_code', 'sub_account_code'
)


class TASLookup(Base):
    """An entry of CARS history -- this TAS was present in the CARS file
    between internal_start_date and internal_end_date (potentially null)
    """
    __tablename__ = "tas_lookup"
    tas_id = Column(Integer, primary_key=True)
    account_num = Column(Integer, index=True, nullable=False)
    allocation_transfer_agency = Column(Text, nullable=True, index=True)
    agency_identifier = Column(Text, nullable=True, index=True)
    beginning_period_of_availa = Column(Text, nullable=True, index=True)
    ending_period_of_availabil = Column(Text, nullable=True, index=True)
    availability_type_code = Column(Text, nullable=True, index=True)
    main_account_code = Column(Text, nullable=True, index=True)
    sub_account_code = Column(Text, nullable=True, index=True)
    internal_start_date = Column(Date, nullable=False)
    internal_end_date = Column(Date, nullable=True)
    financial_indicator2 = Column(Text, nullable=True)
    fr_entity_description = Column(Text, nullable=True)
    fr_entity_type = Column(Text, nullable=True)

    def component_dict(self):
        """We'll often want to copy TAS component fields; this method returns
        a dictionary of field_name to value"""
        return {field_name: getattr(self, field_name) for field_name in TAS_COMPONENTS}

Index("ix_tas",
      TASLookup.allocation_transfer_agency,
      TASLookup.agency_identifier,
      TASLookup.beginning_period_of_availa,
      TASLookup.ending_period_of_availabil,
      TASLookup.availability_type_code,
      TASLookup.main_account_code,
      TASLookup.sub_account_code,
      TASLookup.internal_start_date,
      TASLookup.internal_end_date)


def is_not_distinct_from(left, right):
    """Postgres' IS NOT DISTINCT FROM is an equality check that accounts for
    NULLs. Unfortunately, it doesn't make use of indexes. Instead, we'll
    imitate it here"""
    return sa.or_(left == right, sa.and_(left.is_(None), right.is_(None)))


def matching_cars_subquery(sess, model_class, start_date, end_date):
    """We frequently need to mass-update records to look up their CARS history
    entry. This function creates a subquery to be used in that update call. We
    pass in the database session to avoid circular dependencies"""
    # Why min()?
    # Our data schema doesn't prevent two TAS history entries with the same
    # TAS components (ATA, AI, etc.) from being valid at the same time. When
    # that happens (unlikely), we select the minimum (i.e. older) of the
    # potential TAS history entries.
    subquery = sess.query(sa.func.min(TASLookup.account_num))

    # Filter to matching TAS components, accounting for NULLs
    for field_name in TAS_COMPONENTS:
        tas_col = getattr(TASLookup, field_name)
        model_col = getattr(model_class, field_name)
        subquery = subquery.filter(is_not_distinct_from(tas_col, model_col))

    day_after_end = end_date + timedelta(days=1)
    model_dates = sa.tuple_(start_date, end_date)
    tas_dates = sa.tuple_(TASLookup.internal_start_date, sa.func.coalesce(TASLookup.internal_end_date, day_after_end))
    subquery = subquery.filter(model_dates.op('OVERLAPS')(tas_dates))
    return subquery.as_scalar()


class CGAC(Base):
    __tablename__ = "cgac"
    cgac_id = Column(Integer, primary_key=True)
    cgac_code = Column(Text, nullable=False, index=True, unique=True)
    agency_name = Column(Text)


class FREC(Base):
    __tablename__ = "frec"
    frec_id = Column(Integer, primary_key=True)
    frec_code = Column(Text, nullable=True, index=True, unique=True)
    agency_name = Column(Text)
    cgac_id = Column(Integer, ForeignKey("cgac.cgac_id", name='fk_frec_cgac', ondelete="CASCADE"), nullable=False)
    cgac = relationship('CGAC', foreign_keys='FREC.cgac_id', cascade="delete")


class SubTierAgency(Base):
    __tablename__ = "sub_tier_agency"
    sub_tier_agency_id = Column(Integer, primary_key=True)
    sub_tier_agency_code = Column(Text, nullable=False, index=True, unique=True)
    sub_tier_agency_name = Column(Text)
    cgac_id = Column(Integer, ForeignKey("cgac.cgac_id", name='fk_sub_tier_agency_cgac', ondelete="CASCADE"),
                     nullable=False)
    cgac = relationship('CGAC', foreign_keys='SubTierAgency.cgac_id', cascade="delete")
    priority = Column(Integer, nullable=False, default='2', server_default='2')
    frec_id = Column(Integer, ForeignKey("frec.frec_id", name='fk_sub_tier_agency_frec', ondelete="CASCADE"),
                     nullable=True)
    frec = relationship('FREC', foreign_keys='SubTierAgency.frec_id', cascade="delete")
    is_frec = Column(Boolean, nullable=False, default=False, server_default="False")


class ObjectClass(Base):
    __tablename__ = "object_class"
    object_class_id = Column(Integer, primary_key=True)
    object_class_code = Column(Text, nullable=False, index=True, unique=True)
    object_class_name = Column(Text)


class SF133(Base):
    """Represents GTAS records"""
    __tablename__ = "sf_133"
    sf133_id = Column(Integer, primary_key=True)
    agency_identifier = Column(Text, nullable=False, index=True)
    allocation_transfer_agency = Column(Text, index=True)
    availability_type_code = Column(Text)
    beginning_period_of_availa = Column(Text)
    ending_period_of_availabil = Column(Text)
    main_account_code = Column(Text, nullable=False)
    sub_account_code = Column(Text, nullable=False)
    tas = Column(Text, nullable=False, default=concat_tas, index=True)
    fiscal_year = Column(Integer, nullable=False, index=True)
    period = Column(Integer, nullable=False, index=True)
    line = Column(Integer, nullable=False)
    amount = Column(Numeric, nullable=False, default=0, server_default="0")
    tas_id = Column(Integer, nullable=True)

Index("ix_sf_133_tas_group",
      SF133.tas,
      SF133.fiscal_year,
      SF133.period,
      SF133.line,
      unique=True)


class ProgramActivity(Base):
    __tablename__ = "program_activity"
    program_activity_id = Column(Integer, primary_key=True)
    fiscal_year_quarter = Column(Text, nullable=False, index=True)
    agency_id = Column(Text, nullable=False, index=True)
    allocation_transfer_id = Column(Text)
    account_number = Column(Text, nullable=False, index=True)
    program_activity_code = Column(Text, nullable=False, index=True)
    program_activity_name = Column(Text, nullable=False, index=True)

Index("ix_pa_tas_pa",
      ProgramActivity.fiscal_year_quarter,
      ProgramActivity.agency_id,
      ProgramActivity.allocation_transfer_id,
      ProgramActivity.account_number,
      ProgramActivity.program_activity_code,
      ProgramActivity.program_activity_name,
      unique=True)


class CountryCode(Base):
    __tablename__ = "country_code"
    country_code_id = Column(Integer, primary_key=True)
    country_code = Column(Text, nullable=False, index=True, unique=True)
    country_name = Column(Text, nullable=False)


class ExecutiveCompensation(Base):
    """ File E """
    __tablename__ = "executive_compensation"

    executive_compensation_id = Column(Integer, primary_key=True)
    awardee_or_recipient_uniqu = Column(Text)
    awardee_or_recipient_legal = Column(Text)
    ultimate_parent_unique_ide = Column(Text)
    ultimate_parent_legal_enti = Column(Text)
    high_comp_officer1_full_na = Column(Text)
    high_comp_officer1_amount = Column(Text)
    high_comp_officer2_full_na = Column(Text)
    high_comp_officer2_amount = Column(Text)
    high_comp_officer3_full_na = Column(Text)
    high_comp_officer3_amount = Column(Text)
    high_comp_officer4_full_na = Column(Text)
    high_comp_officer4_amount = Column(Text)
    high_comp_officer5_full_na = Column(Text)
    high_comp_officer5_amount = Column(Text)
    activation_date = Column(Date)
    expiration_date = Column(Date)


class DUNS(Base):
    """ DUNS Records """
    __tablename__ = "duns"

    duns_id = Column(Integer, primary_key=True)
    awardee_or_recipient_uniqu = Column(Text, index=True)
    legal_business_name = Column(Text)
    activation_date = Column(Date, index=True)
    deactivation_date = Column(Date, index=True)
    registration_date = Column(Date, index=True)
    expiration_date = Column(Date, index=True)
    last_sam_mod_date = Column(Date)
    address_line_1 = Column(Text)
    address_line_2 = Column(Text)
    city = Column(Text)
    state = Column(Text)
    zip = Column(Text)
    zip4 = Column(Text)
    country_code = Column(Text)
    congressional_district = Column(Text)
    business_types_codes = Column(ARRAY(Text))
    ultimate_parent_unique_ide = Column(Text)
    ultimate_parent_legal_enti = Column(Text)


class HistoricParentDUNS(Base):
    """ DUNS Records """
    __tablename__ = "historic_parent_duns"

    duns_id = Column(Integer, primary_key=True)
    year = Column(Integer)
    awardee_or_recipient_uniqu = Column(Text, index=True)
    legal_business_name = Column(Text)
    activation_date = Column(Date, index=True)
    deactivation_date = Column(Date, index=True)
    registration_date = Column(Date, index=True)
    expiration_date = Column(Date, index=True)
    last_sam_mod_date = Column(Date)
    ultimate_parent_unique_ide = Column(Text)
    ultimate_parent_legal_enti = Column(Text)


class CFDAProgram(Base):
    __tablename__ = "cfda_program"
    cfda_program_id = Column(Integer, primary_key=True)
    program_number = Column(Float, nullable=False, index=True)
    program_title = Column(Text)
    popular_name = Column(Text)
    federal_agency = Column(Text)
    authorization = Column(Text)
    objectives = Column(Text)
    types_of_assistance = Column(Text)
    uses_and_use_restrictions = Column(Text)
    applicant_eligibility = Column(Text)
    beneficiary_eligibility = Column(Text)
    credentials_documentation = Column(Text)
    preapplication_coordination = Column(Text)
    application_procedures = Column(Text)
    award_procedure = Column(Text)
    deadlines = Column(Text)
    range_of_approval_disapproval_time = Column(Text)
    website_address = Column(Text)
    formula_and_matching_requirements = Column(Text)
    length_and_time_phasing_of_assistance = Column(Text)
    reports = Column(Text)
    audits = Column(Text)
    records = Column(Text)
    account_identification = Column(Text)
    obligations = Column(Text)
    range_and_average_of_financial_assistance = Column(Text)
    appeals = Column(Text)
    renewals = Column(Text)
    program_accomplishments = Column(Text)
    regulations_guidelines_and_literature = Column(Text)
    regional_or_local_office = Column(Text)
    headquarters_office = Column(Text)
    related_programs = Column(Text)
    examples_of_funded_projects = Column(Text)
    criteria_for_selecting_proposals = Column(Text)
    url = Column(Text)
    recovery = Column(Text)
    omb_agency_code = Column(Text)
    omb_bureau_code = Column(Text)
    published_date = Column(Text, index=True)
    archived_date = Column(Text, index=True)


class Zips(Base):
    """ Zip and other address data for validation """
    __tablename__ = "zips"

    zips_id = Column(Integer, primary_key=True)
    zip5 = Column(Text, index=True)
    zip_last4 = Column(Text, index=True)
    state_abbreviation = Column(Text, index=True)
    county_number = Column(Text, index=True)
    congressional_district_no = Column(Text, index=True)

    __table_args__ = (UniqueConstraint('zip5', 'zip_last4', name='uniq_zip5_zip_last4'),)


class CityCode(Base):
    """ City code data and other useful, identifying location data """
    __tablename__ = "city_code"

    city_code_id = Column(Integer, primary_key=True)
    feature_name = Column(Text)
    feature_class = Column(Text)
    city_code = Column(Text, index=True)
    state_code = Column(Text, index=True)
    county_number = Column(Text)
    county_name = Column(Text)
    latitude = Column(Text)
    longitude = Column(Text)


class CountyCode(Base):
    """ County code data per state """
    __tablename__ = "county_code"

    county_code_id = Column(Integer, primary_key=True)
    county_number = Column(Text, index=True)
    county_name = Column(Text)
    state_code = Column(Text, index=True)


class States(Base):
    """ State abbreviations and names """
    __tablename__ = "states"

    states_id = Column(Integer, primary_key=True)
    state_code = Column(Text, index=True)
    state_name = Column(Text)
    fips_code = Column(Text)


class ZipCity(Base):
    """ zip-5 to city name mapping """
    __tablename__ = "zip_city"

    zip_city_id = Column(Integer, primary_key=True)
    zip_code = Column(Text)
    city_name = Column(Text)


class StateCongressional(Base):
    """ state to congressional district mapping """
    __tablename__ = "state_congressional"

    state_congressional_id = Column(Integer, primary_key=True)
    state_code = Column(Text, index=True)
    congressional_district_no = Column(Text, index=True)
    census_year = Column(Integer, index=True)


Index("ix_sc_state_cd",
      StateCongressional.state_code,
      StateCongressional.congressional_district_no,
      unique=True)


class ExternalDataType(Base):
    """ external data type mapping """
    __tablename__ = "external_data_type"

    external_data_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)


class ExternalDataLoadDate(Base):
    """ data load dates corresponding to external data types """
    __tablename__ = "external_data_load_date"

    external_data_load_date_id = Column(Integer, primary_key=True)
    last_load_date = Column(DateTime)
    external_data_type_id = Column(Integer, ForeignKey("external_data_type.external_data_type_id",
                                                       name="fk_external_data_type_id"), unique=True)
    external_data_type = relationship("ExternalDataType", uselist=False)
