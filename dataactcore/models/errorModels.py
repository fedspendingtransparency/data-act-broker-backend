""" These classes define the ORM models to be used by sqlalchemy for the error database """

from sqlalchemy import Column, Integer, Text, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
class Status(Base):
    __tablename__ = "status"
    STATUS_DICT = None

    status_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

class ErrorType(Base):
    __tablename__ = "error_type"
    TYPE_DICT = None

    error_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

class FileStatus(Base):
    __tablename__ = "file_status"

    file_id = Column(Integer, primary_key=True)
    job_id = Column(Integer, nullable=True)
    filename = Column(Text, nullable=True)
    status_id = Column(Integer, ForeignKey("status.status_id"))
    status = relationship("Status", uselist=False)
    headers_missing = Column(Text, nullable=True)
    headers_duplicated = Column(Text, nullable=True)

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

