""" These classes define the ORM models to be used by sqlalchemy for the job tracker database """

from sqlalchemy import Column, Integer, Text, ForeignKey, Boolean, Enum
from sqlalchemy.orm import relationship
from dataactcore.models.baseModel import Base


class FieldType(Base):
    __tablename__ = "field_type"

    field_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

    TYPE_DICT = None


class FileColumn(Base):
    __tablename__ = "file_columns"

    file_column_id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey("file_type.file_type_id", name="fk_file_column_file_type"), nullable=True)
    file = relationship("FileType", uselist=False)
    field_types_id = Column(Integer, ForeignKey("field_type.field_type_id"), nullable=True)
    field_type = relationship("FieldType", uselist=False)
    daims_name = Column(Text, nullable=True)
    name = Column(Text, nullable=True)
    name_short = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    required = Column(Boolean, nullable=True)
    padded_flag = Column(Boolean, default=False, server_default="False", nullable=False)
    length = Column(Integer)


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
    rule_error_message = Column(Text, nullable=False)
    rule_cross_file_flag = Column(Boolean, nullable=False)
    file_id = Column(Integer, ForeignKey("file_type.file_type_id", name="fk_file"), nullable=True)
    file = relationship("FileType", uselist=False, foreign_keys=[file_id])
    rule_severity_id = Column(Integer, ForeignKey("rule_severity.rule_severity_id"), nullable=False)
    rule_severity = relationship("RuleSeverity", uselist=False)
    target_file_id = Column(Integer, ForeignKey("file_type.file_type_id", name="fk_target_file"), nullable=True)
    target_file = relationship("FileType", uselist=False, foreign_keys=[target_file_id])
    query_name = Column(Text)
    expected_value = Column(Text)
    category = Column(Text)


class ValidationLabel(Base):
    __tablename__ = "validation_label"

    validation_label_id = Column(Integer, primary_key=True)
    label = Column(Text)
    error_message = Column(Text)
    file_id = Column(Integer, ForeignKey("file_type.file_type_id", name="fk_file"), nullable=True)
    file = relationship("FileType", uselist=False, foreign_keys=[file_id])
    column_name = Column(Text)
    label_type = Column(Enum('requirement', 'type', name='label_types'))


class RuleSetting(Base):
    __tablename__ = "rule_settings"

    rule_settings_id = Column(Integer, primary_key=True)
    agency_code = Column(Text)
    rule_label = Column(Text, nullable=False)
    file_id = Column(Integer, ForeignKey("file_type.file_type_id", name="fk_setting_file_type"), nullable=True)
    target_file_id = Column(Integer, ForeignKey("file_type.file_type_id", name="fk_setting_target_file_type"),
                            nullable=True)
    priority = Column(Integer, nullable=False)
    impact_id = Column(Integer, ForeignKey("rule_impact.rule_impact_id", ondelete="CASCADE", name="fk_impact"),
                       nullable=False)


class RuleImpact(Base):
    __tablename__ = "rule_impact"

    rule_impact_id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    description = Column(Text, nullable=False)

    IMPACT_DICT = None
