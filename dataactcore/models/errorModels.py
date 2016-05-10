""" These classes define the ORM models to be used by sqlalchemy for the error database """

from sqlalchemy import Column, Integer, Text, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from dataactcore.utils.timeStampMixin import TimeStampBase

Base = declarative_base(cls=TimeStampBase)
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
    job_id = Column(Integer, nullable=True, unique=True)
    filename = Column(Text, nullable=True)
    file_status_id = Column(Integer, ForeignKey("file_status.file_status_id", name="fk_file_status_id"))
    file_status = relationship("FileStatus", uselist=False)
    headers_missing = Column(Text, nullable=True)
    headers_duplicated = Column(Text, nullable=True)
    row_errors_present = Column(Boolean)

class ErrorData(Base):
    __tablename__ = "error_data"

    error_data_id = Column(Integer, primary_key=True)
    job_id = Column(Integer)
    filename = Column(Text, nullable=True)
    field_name = Column(Text)
    error_type_id = Column(Integer, ForeignKey("error_type.error_type_id"), nullable=True)
    error_type = relationship("ErrorType", uselist=False)
    occurrences = Column(Integer)
    first_row = Column(Integer)
    rule_failed = Column(Text, nullable=True)

