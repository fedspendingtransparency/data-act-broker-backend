""" These classes define the ORM models to be used by sqlalchemy for the staging database.  Staging tables for individual jobs are created dynamically, these models represent any other tables """

from sqlalchemy import Column, Integer, Text, ForeignKey, Date, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from dataactcore.utils.timeStampMixin import TimeStampBase

Base = declarative_base(cls=TimeStampBase)

class FieldNameMap(Base):
    __tablename__ = "field_name_map"

    field_name_map_id = Column(Integer, primary_key=True)
    table_name = Column(Text)
    column_to_field_map = Column(Text)