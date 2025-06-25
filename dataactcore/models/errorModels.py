"""These classes define the ORM models to be used by sqlalchemy for the error database"""

from sqlalchemy import Column, Integer, Text, ForeignKey
from sqlalchemy.orm import relationship
from dataactcore.models.baseModel import Base
from dataactcore.models.lookups import FILE_STATUS_DICT_ID


class FileStatus(Base):
    __tablename__ = "file_status"
    FILE_STATUS_DICT = None

    file_status_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)


class ErrorType(Base):
    __tablename__ = "error_type"
    TYPE_DICT = None

    error_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)


class File(Base):
    __tablename__ = "file"

    file_id = Column(Integer, primary_key=True)
    job_id = Column(
        Integer, ForeignKey("job.job_id", name="fk_file_job", ondelete="CASCADE"), nullable=True, unique=True
    )
    job = relationship("Job", uselist=False, cascade="delete")
    filename = Column(Text, nullable=True)
    file_status_id = Column(Integer, ForeignKey("file_status.file_status_id", name="fk_file_status_id"))
    file_status = relationship("FileStatus", uselist=False)
    headers_missing = Column(Text, nullable=True)
    headers_duplicated = Column(Text, nullable=True)

    @property
    def file_status_name(self):
        return FILE_STATUS_DICT_ID.get(self.file_status_id)


class ErrorMetadata(Base):
    __tablename__ = "error_metadata"

    error_metadata_id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("job.job_id", name="fk_error_metadata_job", ondelete="CASCADE"))
    job = relationship("Job", uselist=False, cascade="delete")
    filename = Column(Text, nullable=True)
    field_name = Column(Text)
    error_type_id = Column(Integer, ForeignKey("error_type.error_type_id"), nullable=True)
    error_type = relationship("ErrorType", uselist=False)
    occurrences = Column(Integer)
    first_row = Column(Integer)
    rule_failed = Column(Text, nullable=True)
    file_type_id = Column(Integer, ForeignKey("file_type.file_type_id", name="fk_file_type_file_status_id"))
    file_type = relationship("FileType", foreign_keys=[file_type_id])
    # Second file type id is used in cross file errors
    target_file_type_id = Column(
        Integer, ForeignKey("file_type.file_type_id", name="fk_target_file_type_file_status_id")
    )
    target_file_type = relationship("FileType", foreign_keys=[target_file_type_id])
    original_rule_label = Column(Text, nullable=True)
    severity_id = Column(Integer, ForeignKey("rule_severity.rule_severity_id", name="fk_error_severity_id"))
    severity = relationship("RuleSeverity")


class PublishedErrorMetadata(Base):
    __tablename__ = "published_error_metadata"

    published_error_metadata_id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("job.job_id", name="fk_published_error_metadata_job_id", ondelete="CASCADE"))
    job = relationship("Job", uselist=False, cascade="delete")
    filename = Column(Text, nullable=True)
    field_name = Column(Text)
    error_type_id = Column(Integer, ForeignKey("error_type.error_type_id"), nullable=True)
    error_type = relationship("ErrorType", uselist=False)
    occurrences = Column(Integer)
    first_row = Column(Integer)
    rule_failed = Column(Text, nullable=True)
    file_type_id = Column(
        Integer, ForeignKey("file_type.file_type_id", name="fk_published_error_metadata_file_type_id")
    )
    file_type = relationship("FileType", foreign_keys=[file_type_id])
    # Second file type id is used in cross file errors
    target_file_type_id = Column(
        Integer, ForeignKey("file_type.file_type_id", name="fk_published_error_metadata_target_file_type_id")
    )
    target_file_type = relationship("FileType", foreign_keys=[target_file_type_id])
    original_rule_label = Column(Text, nullable=True)
    severity_id = Column(
        Integer, ForeignKey("rule_severity.rule_severity_id", name="fk_published_error_metadata_severity_id")
    )
    severity = relationship("RuleSeverity")
