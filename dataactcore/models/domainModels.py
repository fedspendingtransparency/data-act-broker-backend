from sqlalchemy.ext.declarative import declarative_base
from dataactcore.utils.timeStampMixin import TimeStampBase
from sqlalchemy import Column, Integer, Text, Boolean

Base = declarative_base(cls=TimeStampBase)

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