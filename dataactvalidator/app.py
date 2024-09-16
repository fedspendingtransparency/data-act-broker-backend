import os
import logging
import csv
import inspect
import time
import traceback
import pandas as pd

from flask import Flask, g, current_app
from opentelemetry import trace

from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider

from dataactcore.aws.sqsHandler import sqs_queue
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status, write_file_error
from dataactcore.broker_logging import configure_logging
from dataactcore.models.jobModels import Job, FileGeneration
from dataactcore.models.lookups import JOB_STATUS_DICT
from dataactcore.utils.ResponseError import ResponseError
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.tracing import OpenTelemetryEagerlyDropTraceFilter, SubprocessTrace
from dataactvalidator.sqs_work_dispatcher import SQSWorkDispatcher

from dataactvalidator.validation_handlers.file_generation_manager import FileGenerationManager
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.validation_handlers.validationManager import ValidationManager
from dataactvalidator.validator_logging import log_job_message

logger = logging.getLogger(__name__)

# Initialize OpenTelemetry
resource = Resource.create(attributes={"service.name": "validator"})
trace.set_tracer_provider(TracerProvider(resource=resource))

if CONFIG_BROKER['local']:
    exporter = ConsoleSpanExporter()
else:
    exporter = OTLPSpanExporter(
        endpoint=os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
    )
span_processor = BatchSpanProcessor(exporter)
trace.get_tracer_provider().add_span_processor(span_processor)
tracer_provider = trace.get_tracer_provider()

READY_STATUSES = [JOB_STATUS_DICT['waiting'], JOB_STATUS_DICT['ready']]
RUNNING_STATUSES = READY_STATUSES + [JOB_STATUS_DICT['running']]
JOB_TYPE = "Validator"


def create_app():
    return Flask(__name__)


def run_app():
    """ Run the application. """
    app = create_app()

    with app.app_context():
        current_app.debug = CONFIG_SERVICES['debug']
        local = CONFIG_BROKER['local']
        g.is_local = local
        current_app.config.from_object(__name__)

        # Future: Override config w/ environment variable, if set
        current_app.config.from_envvar('VALIDATOR_SETTINGS', silent=True)

        queue = sqs_queue()

        logger.info("Starting SQS polling")
        keep_polling = True
        while keep_polling:
            # Start a trace for this poll iter to capture activity in APM
            with SubprocessTrace(
                name=f"job.{JOB_TYPE}",
                attributes={"service": JOB_TYPE.lower(), "resource": queue.url},
            ) as span:
                # Set True to add trace to App Analytics
                span.set_attribute("analytics_sample_rate", 1.0)

                # With cleanup handling engaged, allowing retries
                dispatcher = SQSWorkDispatcher(queue, worker_can_start_child_processes=True)

                def choose_job_by_message_attributes(message):
                    # Determine if this is a retry of this message, in which case job execution should know so it can
                    # do cleanup before proceeding with the job
                    q_msg_attr = message.attributes  # the non-user-defined (queue-defined) attributes on the message
                    is_retry = False
                    if q_msg_attr.get('ApproximateReceiveCount') is not None:
                        is_retry = int(q_msg_attr.get('ApproximateReceiveCount')) > 1

                    msg_attr = message.message_attributes
                    if msg_attr and msg_attr.get('validation_type', {}).get('StringValue') == 'generation':
                        # Generating a file
                        job_signature = {"_job": validator_process_file_generation,
                                         "file_gen_id": message.body,
                                         "is_retry": is_retry}
                    else:
                        # Running validations (or generating a file from a Job)
                        a_agency_code = msg_attr.get('agency_code', {}).get('StringValue') if msg_attr else None
                        job_signature = {"_job": validator_process_job,
                                         "job_id": message.body,
                                         "agency_code": a_agency_code,
                                         "is_retry": is_retry}
                    return job_signature

                found_message = dispatcher.dispatch_by_message_attribute(choose_job_by_message_attributes)

                if not found_message:
                    # When you receive an empty response from the queue, wait before trying again
                    time.sleep(1)

                # If this process is exiting, don't poll for more work
                keep_polling = not dispatcher.is_exiting


def validator_process_file_generation(file_gen_id, is_retry=False):
    """ Retrieves a FileGeneration object based on its ID, and kicks off a file generation. Handles errors by ensuring
        the FileGeneration (if exists) is no longer cached.

        Args:
            file_gen_id: ID of a FileGeneration object
            is_retry: If this is not the very first time handling execution of this job. If True, cleanup is
                      performed before proceeding to retry the job

        Raises:
            Any Exceptions raised by the FileGenerationManager
    """
    # Add args name and values as span tags on this trace
    tag_data = locals()
    with SubprocessTrace(
            name=f"job.{JOB_TYPE}.file_generation",
            attributes={"service": JOB_TYPE.lower()}
    ) as span:
        file_gen_data = {}
        span.set_attributes(tag_data)
        if is_retry:
            if cleanup_generation(file_gen_id):
                log_job_message(
                    logger=logger,
                    message="Attempting a retry of {} after successful retry-cleanup.".format(inspect.stack()[0][3]),
                    job_type=JOB_TYPE,
                    job_id=file_gen_id,
                    is_debug=True
                )
            else:
                log_job_message(
                    logger=logger,
                    message="Retry of {} found to be not necessary after cleanup. "
                            "Returning from job with success.".format(inspect.stack()[0][3]),
                    job_type=JOB_TYPE,
                    job_id=file_gen_id,
                    is_debug=True
                )
                return

        sess = GlobalDB.db().session
        file_generation = None

        try:
            file_generation = sess.query(FileGeneration).filter_by(file_generation_id=file_gen_id).one_or_none()
            if file_generation:
                file_gen_data = {
                    'agency_code': file_generation.agency_code,
                    'agency_type': file_generation.agency_type,
                    'start_date': file_generation.start_date,
                    'end_date': file_generation.end_date,
                    'file_type': file_generation.file_type,
                    'file_path': file_generation.file_path,
                }
                span.resource = f"file_generation/{file_generation.file_type}"
                span.set_attributes(file_gen_data)
            elif file_generation is None:
                raise ResponseError('FileGeneration ID {} not found in database'.format(file_gen_id),
                                    StatusCode.CLIENT_ERROR, None)

            file_generation_manager = FileGenerationManager(sess, g.is_local, file_generation=file_generation)
            file_generation_manager.generate_file()

        except Exception as e:
            # Log uncaught exceptions and fail all Jobs referencing this FileGeneration
            error_data = {
                'message': 'An unhandled exception occurred in the Validator during file generation',
                'message_type': 'ValidatorInfo',
                'file_generation_id': file_gen_id,
                'traceback': traceback.format_exc()
            }
            if file_generation:
                error_data.update(file_gen_data)
            logger.error(error_data)

            # Try to mark the Jobs as failed, but continue raising the original Exception if not possible
            try:
                if file_generation:
                    # Uncache the FileGeneration
                    sess.refresh(file_generation)
                    file_generation.is_cached_file = False

                    # Mark all Jobs waiting on this FileGeneration as failed
                    generation_jobs = sess.query(Job).filter_by(file_generation_id=file_gen_id).all()
                    for job in generation_jobs:
                        if job.job_status in RUNNING_STATUSES:
                            mark_job_status(job.job_id, 'failed')
                            sess.refresh(job)
                            job.file_generation_id = None
                            job.error_message = str(e)
                    sess.commit()
            except Exception:
                pass

            # ResponseErrors only occur at very specific times, and should not affect the Validator's
            # future attempts at handling messages from SQS
            if not isinstance(e, ResponseError):
                raise e


def validator_process_job(job_id, agency_code, is_retry=False):
    """ Retrieves a Job based on its ID, and kicks off a validation. Handles errors by ensuring the Job (if exists) is
        no longer running.

        Args:
            job_id: ID of a Job
            agency_code: CGAC or FREC code for agency, only required for file generations by Job
            is_retry: If this is not the very first time handling execution of this job. If True, cleanup is
                      performed before proceeding to retry the job

        Raises:
            Any Exceptions raised by the GenerationManager or ValidationManager, excluding those explicitly handled
    """
    # Add args name and values as span tags on this trace
    tag_data = locals()
    with SubprocessTrace(
            name=f"job.{JOB_TYPE}.validation",
            attributes={"service": JOB_TYPE.lower()}
    ) as span:
        job_data = {}
        span.set_attributes(tag_data)
        if is_retry:
            if cleanup_validation(job_id):
                log_job_message(
                    logger=logger,
                    message="Attempting a retry of {} after successful retry-cleanup.".format(inspect.stack()[0][3]),
                    job_type=JOB_TYPE,
                    job_id=job_id,
                    is_debug=True
                )
            else:
                log_job_message(
                    logger=logger,
                    message="Retry of {} found to be not necessary after cleanup. "
                            "Returning from job with success.".format(inspect.stack()[0][3]),
                    job_type=JOB_TYPE,
                    job_id=job_id,
                    is_debug=True
                )
                return

        sess = GlobalDB.db().session
        job = None

        try:
            # Get the job
            job = sess.query(Job).filter_by(job_id=job_id).one_or_none()
            if job:
                job_data = {
                    'submission_id': job.submission_id,
                    'job_type': job.job_type.name,
                    'file_type': job.file_type.name if job.file_type else None,
                }
                span.resource = job.job_type.name + (f"/{job.file_type.name}" if job.file_type else "")
                span.set_attributes(job_data)
            elif job is None:
                validation_error_type = ValidationError.job_error
                write_file_error(job_id, None, validation_error_type)
                raise ResponseError('Job ID {} not found in database'.format(job_id),
                                    StatusCode.CLIENT_ERROR, None, validation_error_type)

            mark_job_status(job_id, 'ready')

            # We can either validate or generate a file based on Job ID
            if job.job_type.name == 'file_upload':
                # Generate A, E, or F file
                file_generation_manager = FileGenerationManager(sess, g.is_local, job=job)
                file_generation_manager.generate_file(agency_code)
            else:
                # Run validations
                validation_manager = ValidationManager(g.is_local, CONFIG_SERVICES['error_report_path'])
                validation_manager.validate_job(job.job_id)

        except (ResponseError, csv.Error, pd.errors.ParserError, UnicodeDecodeError, ValueError) as e:
            # Handle exceptions explicitly raised during validation
            error_data = {
                'message': 'An exception occurred in the Validator',
                'message_type': 'ValidatorInfo',
                'job_id': job_id,
                'traceback': traceback.format_exc()
            }

            if job:
                error_data.update(job_data)
                logger.error(error_data)

                sess.refresh(job)
                job.error_message = str(e)
                if job.filename is not None:
                    error_type = ValidationError.unknown_error
                    extra_info = None
                    if isinstance(e, UnicodeDecodeError):
                        error_type = ValidationError.encoding_error
                    elif isinstance(e, ResponseError):
                        error_type = e.errorType
                        extra_info = e.extraInfo

                    write_file_error(job.job_id, job.filename, error_type, extra_info=extra_info)

                mark_job_status(job.job_id, 'invalid')
            else:
                logger.error(error_data)
                raise e

        except Exception as e:
            # Log uncaught exceptions and fail the Job
            error_data = {
                'message': 'An unhandled exception occurred in the Validator',
                'message_type': 'ValidatorInfo',
                'job_id': job_id,
                'traceback': traceback.format_exc()
            }
            if job:
                error_data.update(job_data)
            logger.error(error_data)

            # Try to mark the Job as failed, but continue raising the original Exception if not possible
            try:
                mark_job_status(job_id, 'failed')

                sess.refresh(job)
                job.error_message = str(e)
                sess.commit()
            except Exception:
                pass

            raise e


def cleanup_generation(file_gen_id):
    """ Cleans up generation task if to be reused

        Args:
            file_gen_id: file generation id

        Returns:
            boolean whether or not it needs to be run again
    """
    sess = GlobalDB.db().session
    retry = False

    gen = sess.query(FileGeneration).filter(FileGeneration.file_generation_id == file_gen_id).one_or_none()
    if gen and not gen.file_path:
        retry = True
    elif gen:
        running_jobs = sess.query(Job).filter(Job.file_generation_id == file_gen_id,
                                              Job.job_status_id.in_(RUNNING_STATUSES))
        retry = (running_jobs.count() > 0)
        if retry:
            gen.file_path = None
            gen.is_cached_file = False
            sess.commit()
    return retry


def cleanup_validation(job_id):
    """ Cleans up validation task if to be reused

        Args:
            job_id: ID of a Job

        Returns:
            boolean whether or not it needs to be run again
    """
    sess = GlobalDB.db().session
    retry = False

    job = sess.query(Job).filter(Job.job_id == job_id).one_or_none()
    if job and job.job_status_id in RUNNING_STATUSES:
        if job.job_status_id not in READY_STATUSES:
            job.job_status_id = JOB_STATUS_DICT['waiting']
            sess.commit()
        retry = True
    return retry


if __name__ == "__main__":
    configure_logging()
    # Configure Tracer to drop traces of polls of the queue that have been flagged as uninteresting
    OpenTelemetryEagerlyDropTraceFilter.activate()
    run_app()
