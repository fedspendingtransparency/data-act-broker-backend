from dataactcore.models.stagingModels import concatTas
from sqlalchemy import Column, Integer, Text, Boolean, Index, Numeric
from dataactcore.models.baseModel import Base


class TASLookup(Base) :
    __tablename__ = "tas_lookup"
    tas_id = Column(Integer, primary_key=True)
    allocation_transfer_agency = Column(Text, nullable=True, index=True)
    agency_identifier = Column(Text, nullable=True, index=True)
    beginning_period_of_availability = Column(Text, nullable=True, index=True)
    ending_period_of_availability = Column(Text, nullable=True, index=True)
    availability_type_code = Column(Text, nullable=True, index=True)
    main_account_code = Column(Text, nullable=True, index=True)
    sub_account_code = Column(Text, nullable=True, index=True)

Index("ix_tas",
      TASLookup.allocation_transfer_agency,
      TASLookup.agency_identifier,
      TASLookup.beginning_period_of_availability,
      TASLookup.ending_period_of_availability,
      TASLookup.availability_type_code,
      TASLookup.main_account_code,
      TASLookup.sub_account_code,
      unique=True)

class CGAC(Base):
    __tablename__ = "cgac"
    cgac_id = Column(Integer, primary_key=True)
    cgac_code = Column(Text, nullable=False,index=True,unique=True)
    agency_name = Column(Text)

class ObjectClass(Base):
    __tablename__ = "object_class"
    object_class_id = Column(Integer, primary_key=True)
    object_class_code = Column(Text,nullable=False,index=True,unique=True)
    object_class_name = Column(Text)

class SF133(Base):
    __tablename__ = "sf_133"
    sf133_id = Column(Integer,primary_key=True)
    agency_identifier = Column(Text, nullable=False)
    allocation_transfer_agency = Column(Text)
    availability_type_code = Column(Text)
    beginning_period_of_availa = Column(Text)
    ending_period_of_availabil = Column(Text)
    main_account_code = Column(Text, nullable=False)
    sub_account_code = Column(Text, nullable=False)
    tas = Column(Text, nullable=False, default=concatTas, onupdate=concatTas)
    fiscal_year = Column(Integer, nullable=False)
    period = Column(Integer, nullable=False)
    line = Column(Integer,nullable=False)
    amount = Column(Numeric,nullable=False,default=0,server_default="0")

Index("ix_sf_133_tas",
  SF133.tas,
  SF133.period,
  SF133.line,
  unique=True)

class ProgramActivity(Base):
    __tablename__ = "program_activity"
    program_activity_id = Column(Integer, primary_key=True)
    budget_year = Column(Text,nullable=False)
    agency_id = Column(Text,nullable=False)
    allocation_transfer_id = Column(Text)
    account_number = Column(Text,nullable=False)
    program_activity_code = Column(Text,nullable=False)
    program_activity_name = Column(Text,nullable=False)

Index("ix_pa_tas_pa",
      ProgramActivity.budget_year,
      ProgramActivity.agency_id,
      ProgramActivity.allocation_transfer_id,
      ProgramActivity.account_number,
      ProgramActivity.program_activity_code,
      ProgramActivity.program_activity_name,
      unique=True)
