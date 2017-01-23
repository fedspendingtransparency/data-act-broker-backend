""" These classes define the ORM models to be used by sqlalchemy for the job tracker database """

from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, Text, UniqueConstraint
from sqlalchemy.orm import relationship
from dataactcore.models.baseModel import Base
from dataactcore.models.lookups import FILE_TYPE_DICT_ID, JOB_STATUS_DICT_ID, JOB_TYPE_DICT_ID


def generate_fiscal_year(context):
    """ Generate fiscal year based on the date provided """
    reporting_end_date = context.current_parameters['reporting_end_date']
    year = reporting_end_date.year
    if reporting_end_date.month in [10, 11, 12]:
        year += 1
    return year


def generate_fiscal_period(context):
    """ Generate fiscal period based on the date provided """
    reporting_end_date = context.current_parameters['reporting_end_date']
    period = (reporting_end_date.month + 3) % 12
    period = 12 if period == 0 else period
    return period


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


class PublishStatus(Base):
    __tablename__ = "publish_status"
    PUBLISH_STATUS_DICT = None

    publish_status_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)


class Submission(Base):
    __tablename__ = "submission"

    submission_id = Column(Integer, primary_key=True)
    datetime_utc = Column(DateTime)
    user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL", name="fk_submission_user"),
                     nullable=True)
    user = relationship("User")
    cgac_code = Column(Text)
    reporting_start_date = Column(Date, nullable=False)
    reporting_end_date = Column(Date, nullable=False)
    reporting_fiscal_year = Column(Integer, nullable=False, default=generate_fiscal_year, server_default='0')
    reporting_fiscal_period = Column(Integer, nullable=False, default=generate_fiscal_period, server_default='0')
    is_quarter_format = Column(Boolean, nullable=False, default="False", server_default="False")
    jobs = None
    publishable = Column(Boolean, nullable=False, default="False", server_default="False")
    publish_status_id = Column(Integer, ForeignKey("publish_status.publish_status_id", ondelete="SET NULL",
                                                   name="fk_publish_status_id"))
    publish_status = relationship("PublishStatus", uselist=False)
    number_of_errors = Column(Integer, nullable=False, default=0, server_default='0')
    number_of_warnings = Column(Integer, nullable=False, default=0, server_default='0')


class Job(Base):
    __tablename__ = "job"

    job_id = Column(Integer, primary_key=True)
    filename = Column(Text, nullable=True)
    job_status_id = Column(Integer, ForeignKey("job_status.job_status_id", name="fk_job_status_id"))
    job_status = relationship("JobStatus", uselist=False, lazy='joined')
    job_type_id = Column(Integer, ForeignKey("job_type.job_type_id", name="fk_job_type_id"))
    job_type = relationship("JobType", uselist=False, lazy='joined')
    submission_id = Column(Integer,
                           ForeignKey("submission.submission_id", ondelete="CASCADE", name="fk_job_submission_id"))
    submission = relationship("Submission", uselist=False, cascade="delete")
    file_type_id = Column(Integer, ForeignKey("file_type.file_type_id"), nullable=True)
    file_type = relationship("FileType", uselist=False, lazy='joined')
    original_filename = Column(Text, nullable=True)
    file_size = Column(Integer)
    number_of_rows = Column(Integer)
    number_of_rows_valid = Column(Integer)
    number_of_errors = Column(Integer, nullable=False, default=0, server_default='0')
    number_of_warnings = Column(Integer, nullable=False, default=0, server_default='0')
    error_message = Column(Text)
    start_date = Column(Date)
    end_date = Column(Date)
    user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL", name="fk_job_user"), nullable=True)

    @property
    def job_type_name(self):
        return JOB_TYPE_DICT_ID.get(self.job_type_id)

    @property
    def job_status_name(self):
        return JOB_STATUS_DICT_ID.get(self.job_status_id)

    @property
    def file_type_name(self):
        return FILE_TYPE_DICT_ID.get(self.file_type_id)


class JobDependency(Base):
    __tablename__ = "job_dependency"

    dependency_id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("job.job_id", name="fk_dep_job_id"))
    prerequisite_id = Column(Integer, ForeignKey("job.job_id", name="fk_prereq_job_id"))
    dependent_job = relationship("Job", foreign_keys=[job_id], lazy='joined')
    prerequisite_job = relationship("Job", foreign_keys=[prerequisite_id], lazy='joined')


class FileType(Base):
    __tablename__ = "file_type"
    FILE_TYPE_DICT = None

    file_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)
    letter_name = Column(Text)
    file_order = Column(Integer)


class FileGenerationTask(Base):
    __tablename__ = "file_generation_task"

    file_generation_task_id = Column(Integer, primary_key=True)
    generation_task_key = Column(Text, index=True, unique=True)
    job_id = Column(Integer, ForeignKey("job.job_id", name="fk_generation_job"))
    job = relationship("Job", uselist=False, cascade="delete")


class SubmissionNarrative(Base):
    __tablename__ = "submission_narrative"

    submission_narrative_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey("submission.submission_id", name="fk_submission"), nullable=False)
    submission = relationship(Submission, uselist=False)
    file_type_id = Column(Integer, ForeignKey("file_type.file_type_id", name="fk_file_type"), nullable=False)
    file_type = relationship(FileType, uselist=False)
    narrative = Column(Text, nullable=False)

    __table_args__ = (UniqueConstraint('submission_id', 'file_type_id', name='uniq_submission_file_type'),)


class SQS(Base):
    __tablename__ = "sqs"

    sqs_id = Column(Integer, primary_key=True)
    job_id = Column(Integer, nullable=False)

    __table_args__ = (UniqueConstraint('job_id', name='uniq_job_id'),)
