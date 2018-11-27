import logging
import csv
import traceback

from flask import Flask, g, current_app

from dataactcore.aws.sqsHandler import sqs_queue
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status, write_file_error
from dataactcore.logging import configure_logging
from dataactcore.models.jobModels import Job, FileGeneration
from dataactcore.models.lookups import JOB_STATUS_DICT
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

from dataactvalidator.validation_handlers.file_generation_manager import FileGenerationManager
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.validation_handlers.validationManager import ValidationManager

# DataDog Import (the below value gets changed via Ansible during deployment. DO NOT DELETE)
USE_DATADOG = False

if USE_DATADOG:
    from ddtrace import tracer
    from ddtrace.contrib.flask import TraceMiddleware

logger = logging.getLogger(__name__)


def create_app():
    return Flask(__name__)


def run_app():
    """Run the application."""
    app = create_app()

    # This is for DataDog (Do Not Delete)
    if USE_DATADOG:
        TraceMiddleware(app, tracer, service="broker-dd", distributed_tracing=False)

    with app.app_context():
        current_app.debug = CONFIG_SERVICES['debug']
        local = CONFIG_BROKER['local']
        g.is_local = local
        current_app.config.from_object(__name__)

        # Future: Override config w/ environment variable, if set
        current_app.config.from_envvar('VALIDATOR_SETTINGS', silent=True)

        queue = sqs_queue()
        messages = []

        logger.info("Starting SQS polling")
        while True:
            # Set current_message to None before every loop to ensure it's never set to the previous message
            current_message = None
            try:
                # Grabs one (or more) messages from the queue
                messages = queue.receive_messages(WaitTimeSeconds=10, MessageAttributeNames=['All'])
                for message in messages:
                    logger.info("Message received: %s", message.body)

                    # Retrieve the message body and attributes
                    current_message = message
                    msg_attr = current_message.message_attributes

                    # Generating a file
                    if msg_attr and msg_attr.get('validation_type', {}).get('StringValue') == 'generation':
                        handled_error = validator_process_file_generation(message.body)
                    # Running validations
                    else:
                        a_agency_code = msg_attr.get('agency_code', {}).get('StringValue') if msg_attr else None
                        handled_error = validator_process_job(message.body, current_message, a_agency_code)

                    # Delete from SQS once processed
                    if not handled_error:
                        message.delete()

            except Exception as e:
                # Log exceptions
                logger.error(traceback.format_exc())


def validator_process_file_generation(file_gen_id):
    """ Retrieves a FileGeneration object based on its ID, and kicks off a file generation. Handles errors by ensuring
        the FileGeneration (if exists) is no longer cached.

    Args:
        file_gen_id: ID of a FileGeneration object

    Raises:
        Any Exceptions raised by the FileGenerationManager
    """
    sess = GlobalDB.db().session
    file_generation = None
    has_errors = False

    try:
        file_generation = sess.query(FileGeneration).filter_by(file_generation_id=file_gen_id).one_or_none()
        if file_generation is None:
            raise ResponseException('FileGeneration ID {} not found in database'.format(file_gen_id),
                                    StatusCode.CLIENT_ERROR, None)

        file_generation_manager = FileGenerationManager(sess, g.is_local, file_generation=file_generation)
        file_generation_manager.generate_file()

    except Exception as e:
        logger.error(traceback.format_exc())
        has_errors = True

        if file_generation:
            # Uncache the FileGeneration
            sess.refresh(file_generation)
            file_generation.is_cached_file = False

            # Mark all Jobs waiting on this FileGeneration as failed
            generation_jobs = sess.query(Job).filter_by(file_generation_id=file_gen_id).all()
            for job in generation_jobs:
                if job.job_status in [JOB_STATUS_DICT['waiting'], JOB_STATUS_DICT['ready'], JOB_STATUS_DICT['running']]:
                    mark_job_status(job.job_id, 'failed')
                    sess.refresh(job)
                    job.update({'file_generation_id': None}, synchronize_session=False)

            sess.commit()

    return has_errors


def validator_process_job(job_id, current_message, agency_code):
    """ Retrieves a Job based on its ID, and kicks off a validation. Handles errors by ensuring the Job (if exists) is
        no longer running.

    Args:
        job_id: ID of a Job
        current_message: The currently in-transit SQS message

    Raises:
        Any Exceptions raised by the ValidationManager
    """
    sess = GlobalDB.db().session
    job = None
    has_errors = False
    try:
        mark_job_status(job_id, 'ready')

        # Get the job
        job = sess.query(Job).filter_by(job_id=job_id).one_or_none()
        if job is None:
            validation_error_type = ValidationError.jobError
            write_file_error(job_id, None, validation_error_type)
            raise ResponseException('Job ID {} not found in database'.format(job_id),
                                    StatusCode.CLIENT_ERROR, None, validation_error_type)

        # We can either validate or generate a file based on Job ID
        if job.job_type.name == 'file_upload':
            # Generate A, E, or F file
            file_generation_manager = FileGenerationManager(sess, g.is_local, job=job)
            file_generation_manager.generate_file(agency_code)
        else:
            # Run validations
            validation_manager = ValidationManager(g.is_local, CONFIG_SERVICES['error_report_path'])
            validation_manager.validate_job(job.job_id)

    except ResponseException as e:
        # Handle exceptions explicitly raised during validation.
        logger.error(traceback.format_exc())
        has_errors = True

        if job:
            sess.refresh(job)
            if job.filename is not None:
                # Insert file-level error info to the database
                write_file_error(job.job_id, job.filename, e.errorType, e.extraInfo)

            if e.errorType != ValidationError.jobError:
                # Job passed prerequisites for validation but an error happened somewhere: mark job as 'invalid'
                mark_job_status(job.job_id, 'invalid')
                if current_message:
                    if e.errorType in [ValidationError.rowCountError, ValidationError.headerError,
                                       ValidationError.fileTypeError]:
                        current_message.delete()

    except Exception as e:
        # Handle uncaught exceptions in validation process.
        logger.error(traceback.format_exc())
        has_errors = True

        if job:
            # csv-specific errors get a different job status and response code
            sess.refresh(job)
            if isinstance(e, ValueError) or isinstance(e, csv.Error) or isinstance(e, UnicodeDecodeError):
                job_status = 'invalid'
            else:
                job_status = 'failed'

            if job.filename is not None:
                error_type = ValidationError.unknownError

                # Delete UnicodeDecodeErrors because they're the only legitimate errors caught as exceptions
                if isinstance(e, UnicodeDecodeError):
                    error_type = ValidationError.encodingError
                    if current_message:
                        current_message.delete()

                write_file_error(job.job_id, job.filename, error_type)

            mark_job_status(job.job_id, job_status)

    return has_errors


if __name__ == "__main__":
    configure_logging()
    run_app()
