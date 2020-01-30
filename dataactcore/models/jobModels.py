""" These classes define the ORM models to be used by sqlalchemy for the job tracker database """
from datetime import datetime
from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, Text, UniqueConstraint, Enum, BigInteger
from sqlalchemy.orm import relationship
from dataactcore.models.baseModel import Base
from dataactcore.models.domainModels import SubTierAgency
from dataactcore.models.lookups import FILE_TYPE_DICT_ID, JOB_STATUS_DICT_ID, JOB_TYPE_DICT_ID


def generate_fiscal_year(context):
    """ Generate fiscal year based on the date provided """
    reporting_end_date = context.current_parameters['reporting_end_date']
    year = 0
    if reporting_end_date:
        year = reporting_end_date.year
        if reporting_end_date.month in [10, 11, 12]:
            year += 1
    return year


def generate_fiscal_period(context):
    """ Generate fiscal period based on the date provided """
    reporting_end_date = context.current_parameters['reporting_end_date']
    period = 0
    if reporting_end_date:
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
    user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL", name="fk_submission_user"),
                     nullable=True)
    user = relationship("User", foreign_keys=[user_id])
    cgac_code = Column(Text)
    frec_code = Column(Text)
    reporting_start_date = Column(Date)
    reporting_end_date = Column(Date)
    reporting_fiscal_year = Column(Integer, nullable=False, default=generate_fiscal_year, server_default='0')
    reporting_fiscal_period = Column(Integer, nullable=False, default=generate_fiscal_period, server_default='0')
    is_quarter_format = Column(Boolean, nullable=False, default=False, server_default="False")
    jobs = None
    publishable = Column(Boolean, nullable=False, default=False, server_default="False")
    publish_status_id = Column(Integer, ForeignKey("publish_status.publish_status_id", ondelete="SET NULL",
                                                   name="fk_publish_status_id"))
    publish_status = relationship("PublishStatus", uselist=False)
    number_of_errors = Column(Integer, nullable=False, default=0, server_default='0')
    number_of_warnings = Column(Integer, nullable=False, default=0, server_default='0')
    d2_submission = Column(Boolean, nullable=False, default=False, server_default="False")
    certifying_user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL",
                                                    name="fk_submission_certifying_user"),
                                nullable=True)
    certifying_user = relationship("User", foreign_keys=[certifying_user_id])


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
    file_size = Column(BigInteger)
    number_of_rows = Column(Integer)
    number_of_rows_valid = Column(Integer)
    number_of_errors = Column(Integer, nullable=False, default=0, server_default='0')
    number_of_warnings = Column(Integer, nullable=False, default=0, server_default='0')
    error_message = Column(Text)
    start_date = Column(Date)
    end_date = Column(Date)
    user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL", name="fk_job_user"), nullable=True)
    last_validated = Column(DateTime, default=datetime.utcnow)
    file_generation_id = Column(Integer, ForeignKey("file_generation.file_generation_id", ondelete="SET NULL",
                                                    name="fk_file_request_file_generation_id"), nullable=True)
    file_generation = relationship("FileGeneration", uselist=False)

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
    job_id = Column(Integer, ForeignKey("job.job_id", name="fk_dep_job_id", ondelete="CASCADE"))
    prerequisite_id = Column(Integer, ForeignKey("job.job_id", name="fk_prereq_job_id", ondelete="CASCADE"))
    dependent_job = relationship("Job", foreign_keys=[job_id], lazy='joined', cascade="delete")
    prerequisite_job = relationship("Job", foreign_keys=[prerequisite_id], lazy='joined', cascade="delete")


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
    job_id = Column(Integer, ForeignKey("job.job_id", name="fk_generation_job", ondelete="CASCADE"))
    job = relationship("Job", uselist=False, cascade="delete")


class Comment(Base):
    __tablename__ = "comment"

    comment_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey("submission.submission_id", name="fk_submission", ondelete="CASCADE"),
                           nullable=False)
    submission = relationship(Submission, uselist=False, cascade="delete")
    file_type_id = Column(Integer, ForeignKey("file_type.file_type_id", name="fk_file_type"), nullable=False)
    file_type = relationship(FileType, uselist=False)
    comment = Column(Text, nullable=False)

    __table_args__ = (UniqueConstraint('submission_id', 'file_type_id', name='uniq_submission_file_type'),)


class CertifiedComment(Base):
    __tablename__ = "certified_comment"

    certified_comment_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey("submission.submission_id", name="fk_submission", ondelete="CASCADE"),
                           nullable=False)
    submission = relationship(Submission, uselist=False, cascade="delete")
    file_type_id = Column(Integer, ForeignKey("file_type.file_type_id", name="fk_file_type"), nullable=False)
    file_type = relationship(FileType, uselist=False)
    comment = Column(Text, nullable=False)

    __table_args__ = (UniqueConstraint('submission_id', 'file_type_id', name='uniq_cert_comment_submission_file_type'),)


class SubmissionSubTierAffiliation(Base):
    __tablename__ = "submission_sub_tier_affiliation"

    submission_sub_tier_affiliation_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey("submission.submission_id",
                                               name="fk_submission_sub_tier_affiliation_id"))
    submission = relationship(Submission, uselist=False)
    sub_tier_agency_id = Column(Integer, ForeignKey("sub_tier_agency.sub_tier_agency_id",
                                                    name="fk_sub_tier_submission_affiliation_agency_id"))
    sub_tier_agency = relationship(SubTierAgency, uselist=False)


class SQS(Base):
    __tablename__ = "sqs"

    sqs_id = Column(Integer, primary_key=True)
    message = Column(Integer, nullable=False)
    attributes = Column(Text, nullable=True)


class RevalidationThreshold(Base):
    __tablename__ = "revalidation_threshold"

    revalidation_date = Column(DateTime, primary_key=True)


class QuarterlyRevalidationThreshold(Base):
    __tablename__ = "quarterly_revalidation_threshold"

    quarterly_revalidation_threshold_id = Column(Integer, primary_key=True)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    window_start = Column(DateTime)
    window_end = Column(DateTime)


class FPDSUpdate(Base):
    __tablename__ = "fpds_update"

    fpds_update_id = Column(Integer, server_default='1', default=1, primary_key=True)
    update_date = Column(Date)


class CertifyHistory(Base):
    __tablename__ = "certify_history"

    certify_history_id = Column(Integer, primary_key=True)
    submission_id = Column(Integer, ForeignKey("submission.submission_id", name="fk_certify_history_submission_id"))
    submission = relationship("Submission", uselist=False)
    user_id = Column(Integer, ForeignKey("users.user_id", ondelete="SET NULL", name="fk_certify_history_user"),
                     nullable=True)
    user = relationship("User")


class CertifiedFilesHistory(Base):
    __tablename__ = "certified_files_history"

    certified_files_history_id = Column(Integer, primary_key=True)
    certify_history_id = Column(Integer, ForeignKey("certify_history.certify_history_id",
                                                    name="fk_certify_history_certified_files_id"))
    certify_history = relationship("CertifyHistory", uselist=False)
    submission_id = Column(Integer, ForeignKey("submission.submission_id",
                                               name="fk_certified_files_history_submission_id"))
    submission = relationship("Submission", uselist=False)
    filename = Column(Text)
    file_type_id = Column(Integer, ForeignKey("file_type.file_type_id", name="fk_certified_files_history_file_type"),
                          nullable=True,)
    file_type = relationship("FileType", uselist=False, lazy='joined')
    warning_filename = Column(Text)
    comment = Column(Text)


class SubmissionWindow(Base):
    __tablename__ = "submission_window"

    window_id = Column(Integer, primary_key=True)
    start_date = Column(Date)
    end_date = Column(Date)
    block_certification = Column(Boolean, default=False)
    header = Column(Text)
    message = Column(Text)
    banner_type = Column(Text, default="warning", nullable=False)
    application_type_id = Column(Integer, ForeignKey("application_type.application_type_id",
                                 name="fk_submission_window_application"))
    application_type = relationship("ApplicationType")


class ApplicationType(Base):
    __tablename__ = "application_type"

    application_type_id = Column(Integer, primary_key=True)
    application_name = Column(Text, nullable=False)


class FileGeneration(Base):
    __tablename__ = "file_generation"

    file_generation_id = Column(Integer, primary_key=True)
    request_date = Column(Date, nullable=False, index=True)
    start_date = Column(Date, nullable=False, index=True)
    end_date = Column(Date, nullable=False, index=True)
    agency_code = Column(Text, nullable=False, index=True)
    agency_type = Column(Enum('awarding', 'funding', name='generation_agency_types'), nullable=False, index=True,
                         default='awarding', server_default='awarding')
    file_type = Column(Enum('D1', 'D2', name='generation_file_types'), nullable=False, index=True,
                       default='D1', server_default='D1')
    file_path = Column(Text)
    is_cached_file = Column(Boolean, nullable=False, default=False)
    file_format = Column(Enum('csv', 'txt', name='generation_file_format'), nullable=False, index=True,
                         default='csv', server_default='csv')
