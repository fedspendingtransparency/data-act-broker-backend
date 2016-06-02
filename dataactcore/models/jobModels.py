""" These classes define the ORM models to be used by sqlalchemy for the job tracker database """

from sqlalchemy import Column, Integer, Text, ForeignKey, Date, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from dataactcore.utils.timeStampMixin import TimeStampBase

Base = declarative_base(cls=TimeStampBase)

class JobStatus(Base):
    __tablename__ = "job_status"
    JOB_STATUS_DICT = None

    job_status_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

class JobType(Base):
    __tablename__ = "job_type"
    JOB_TYPE_DICT = None

    job_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

class Submission(Base):
    __tablename__ = "submission"

    submission_id = Column(Integer, primary_key=True)
    datetime_utc = Column(DateTime)
    user_id = Column(Integer, nullable=False) # This refers to the users table in the User DB
    agency_name = Column(Text)
    reporting_start_date = Column(Date)
    reporting_end_date = Column(Date)
    is_quarter_format = Column(Boolean, nullable = False, default = "False", server_default= "False")
    jobs = None

class Job(Base):
    __tablename__ = "job"

    job_id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=True)
    job_status_id = Column(Integer, ForeignKey("job_status.job_status_id", name="fk_job_status_id"))
    job_status = relationship("JobStatus", uselist=False)
    job_type_id = Column(Integer, ForeignKey("job_type.job_type_id", name="fk_job_type_id"))
    job_type = relationship("JobType", uselist=False)
    submission_id = Column(Integer, ForeignKey("submission.submission_id", ondelete="CASCADE", name="fk_job_submission_id"))
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
    job_id = Column(Integer, ForeignKey("job.job_id"))
    prerequisite_id = Column(Integer, ForeignKey("job.job_id"))

class FileType(Base):
    __tablename__ = "file_type"

    file_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)
