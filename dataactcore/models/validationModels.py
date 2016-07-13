""" These classes define the ORM models to be used by sqlalchemy for the job tracker database """

from sqlalchemy import Column, Integer, Text, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from dataactcore.models.validationBase import Base
# the following lines import models that are maintained in
# separate files but live in the validation database.
# they're imported here so alembic will see them.
import dataactcore.models.domainModels as domainModels
import dataactcore.models.stagingModels as stagingModels

class FileType(Base):
    __tablename__ = "file_type"

    file_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)
    file_order = Column(Integer, nullable=False, server_default="0")

    TYPE_DICT = None
    TYPE_ID_DICT = None

class FieldType(Base):
    __tablename__ = "field_type"

    field_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

    TYPE_DICT = None

class RuleType(Base):
    __tablename__ = "rule_type"

    rule_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

    TYPE_DICT = None

class FileColumn(Base):
    __tablename__ = "file_columns"

    file_column_id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey("file_type.file_id"), nullable=True)
    file = relationship("FileType", uselist=False)
    field_types_id = Column(Integer, ForeignKey("field_type.field_type_id"), nullable=True)
    field_type = relationship("FieldType", uselist=False)
    name = Column(Text, nullable=True)
    name_short = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    required = Column(Boolean, nullable=True)
    rules = relationship("Rule", cascade = "delete, delete-orphan")
    padded_flag = Column(Boolean, default=False, server_default="False", nullable=False)

class Rule(Base):
    __tablename__ = "rule"

    rule_id = Column(Integer, primary_key=True)
    file_column_id = Column(Integer, ForeignKey("file_columns.file_column_id"), nullable=True)
    rule_type_id  = Column(Integer, ForeignKey("rule_type.rule_type_id"), nullable=True)
    rule_text_1 = Column(Text, nullable=True)
    rule_text_2 = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    rule_type = relationship("RuleType", uselist=False)
    file_column = relationship("FileColumn", uselist=False)
    rule_timing_id = Column(Integer, ForeignKey("rule_timing.rule_timing_id", name="fk_rule_timing_id"), nullable=False, default=1)
    rule_timing = relationship("RuleTiming", uselist=False)
    rule_label = Column(Text)
    original_label = Column(Text)
    file_id = Column(Integer, ForeignKey("file_type.file_id"), nullable=True)
    file_type = relationship(
        "FileType", foreign_keys="Rule.file_id", uselist=False)
    target_file_id = Column(Integer, ForeignKey(
        "file_type.file_id", name="fk_target_file_id"), nullable=True)
    target_file_type = relationship(
        "FileType", foreign_keys="Rule.target_file_id", uselist=False)

class RuleTiming(Base):
    __tablename__ = "rule_timing"

    rule_timing_id = Column(Integer, primary_key=True)
    name = Column(Text,nullable=False)
    description = Column(Text, nullable=False)

    TIMING_DICT = None

class RuleSeverity(Base):
    __tablename__ = "rule_severity"

    rule_severity_id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    description = Column(Text, nullable=False)

    SEVERITY_DICT = None

class RuleSql(Base):
    __tablename__ = "rule_sql"

    rule_sql_id = Column(Integer, primary_key=True)
    rule_sql = Column(Text, nullable=False)
    rule_label = Column(Text)
    rule_description = Column(Text, nullable=False)
    rule_error_message = Column(Text, nullable=False)
    rule_cross_file_flag = Column(Boolean, nullable=False)
    file_id = Column(Integer, ForeignKey("file_type.file_id", name="fk_file"), nullable=True)
    file = relationship("FileType", uselist=False, foreign_keys=[file_id])
    rule_severity_id = Column(Integer, ForeignKey("rule_severity.rule_severity_id"), nullable=False)
    rule_severity = relationship("RuleSeverity", uselist=False)
    target_file_id = Column(Integer, ForeignKey("file_type.file_id", name="fk_target_file"), nullable=True)
    target_file = relationship("FileType", uselist=False, foreign_keys=[target_file_id])
    query_name = Column(Text)
