""" These classes define the ORM models to be used by sqlalchemy for the job tracker database """

from sqlalchemy import Column, Integer, Text, ForeignKey, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Status(Base):
    __tablename__ = "status"
    STATUS_DICT = None

    status_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

class Type(Base):
    __tablename__ = "type"
    TYPE_DICT = None
    TYPE_LIST = ["file_upload", "csv_record_validation","db_transfer","validation","external_validation"]

    type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

class Submission(Base):
    __tablename__ = "submission"

    submission_id = Column(Integer, primary_key=True)
    datetime_utc = Column(Text)
    user_id = Column(Integer, nullable=False) # This refers to the users table in the User DB
    agency_name = Column(Text)
    reporting_start_date = Column(Date)
    reporting_end_date = Column(Date)
    jobs = None

class JobStatus(Base):
    __tablename__ = "job_status"

    job_id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=True)
    status_id = Column(Integer, ForeignKey("status.status_id"))
    status = relationship("Status", uselist=False)
    type_id = Column(Integer, ForeignKey("type.type_id"))
    type = relationship("Type", uselist=False)
    submission_id = Column(Integer, ForeignKey("submission.submission_id", ondelete="CASCADE"))
    submission = relationship("Submission", uselist=False, cascade="delete")
    file_type_id = Column(Integer, ForeignKey("file_type.file_type_id"), nullable=True)
    file_type = relationship("FileType", uselist=False)
    staging_table = Column(Text, nullable=True)
    original_filename = Column(Text, nullable=True)
    file_size = Column(Integer)
    number_of_rows = Column(Integer)

class JobDependency(Base):
    __tablename__ = "job_dependency"

    dependency_id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("job_status.job_id"))
    #job_status = relationship("JobStatus")
    prerequisite_id = Column(Integer, ForeignKey("job_status.job_id"))
    #prerequisite_status = relationship("JobStatus")

class FileType(Base):
    __tablename__ = "file_type"

    file_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)
