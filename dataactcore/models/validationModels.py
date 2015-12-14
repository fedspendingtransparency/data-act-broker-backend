""" These classes define the ORM models to be used by sqlalchemy for the job tracker database """

import sqlalchemy
from sqlalchemy import Column, Integer, Text, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from dataactcore.models.jobTrackerInterface import JobTrackerInterface


Base = declarative_base()

class Rule(Base):
    __tablename__ = "rule"
    #__table__ = "rule"

    rule_id = Column(Integer, primary_key=True)
    file_column_id = Column(Integer, ForeignKey("file_columns.file_column_id"))
    file_columns = relationship("FileColumn", back_populates="file_columns")
    rule_type_id = Column(Integer, ForeignKey("rule_type.rule_type_id"))
    rule_text_1 = Column(Text)
    rule_text_2 = Column(Text)
    rule_type = relationship("RuleType", back_populates="rule_type")
    file_column = relationship("FileColumn", back_populates="file_columns")

class RuleType(Base):
    __tablename__ = "rule_type"

    rule_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

class FileColumn(Base):
    __tablename__ = "file_columns"

    file_column_id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey("file_type.file_id"))
    file = relationship("FileType", back_populates="file_type")
    field_types_id = Column(Integer, ForeignKey("field_type.field_type_id"))
    field_type = relationship("FieldType", back_populates="field_type")
    name = Column(Text)
    description = Column(Text)
    required = Column(Boolean)

class FieldType(Base):
    __tablename__ = "field_type"

    field_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

class FileType(Base):
    __tablename__ = "file_type"

    file_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)
