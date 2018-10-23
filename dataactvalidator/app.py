import logging
import csv
import traceback

from botocore.exceptions import ClientError
from flask import Flask, g, current_app

from dataactcore.aws.sqsHandler import sqs_queue
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status, write_file_error
from dataactcore.logging import configure_logging
from dataactcore.models.jobModels import Job
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
        error_report_path = CONFIG_SERVICES['error_report_path']
        current_app.config.from_object(__name__)

        # Create connection to job tracker database
        sess = GlobalDB.db().session

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

                    # Retrieve the job_id from the message body
                    current_message = message
                    g.job_id = message.body
                    mark_job_status(g.job_id, "ready")

                    # Get the job
                    job = sess.query(Job).filter_by(job_id=g.job_id).one_or_none()
                    if job is None:
                        validation_error_type = ValidationError.jobError
                        write_file_error(g.job_id, None, validation_error_type)
                        raise ResponseException('Job ID {} not found in database'.format(g.job_id),
                                                StatusCode.CLIENT_ERROR, None, validation_error_type)

                    # We have two major functionalities in the Validator: validation and file generation
                    if not job.file_type or job.file_type.letter_name in ['A', 'B', 'C', 'FABS'] or \
                       job.job_type.name != 'file_upload':
                        # Run validations
                        validation_manager = ValidationManager(local, error_report_path)
                        validation_manager.validate_job(job.job_id)
                    else:
                        # Retrieve the agency code data from the message attributes
                        msg_attr = current_message.message_attributes
                        agency_code = msg_attr['agency_code']['StringValue'] if msg_attr and \
                            msg_attr.get('agency_code') else None
                        agency_type = msg_attr['agency_type']['StringValue'] if msg_attr and \
                            msg_attr.get('agency_type') else None

                        file_generation_manager = FileGenerationManager(job, agency_code, agency_type, local)
                        file_generation_manager.generate_from_job()
                        sess.commit()
                        sess.refresh(job)

                    # Delete from SQS once processed
                    message.delete()

            except ResponseException as e:
                # Handle exceptions explicitly raised during validation.
                logger.error(traceback.format_exc())

                job = get_current_job()
                if job:
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

                # csv-specific errors get a different job status and response code
                if isinstance(e, ValueError) or isinstance(e, csv.Error) or isinstance(e, UnicodeDecodeError):
                    job_status = 'invalid'
                else:
                    job_status = 'failed'
                job = get_current_job()
                if job:
                    if job.filename is not None:
                        error_type = ValidationError.unknownError
                        if isinstance(e, UnicodeDecodeError):
                            error_type = ValidationError.encodingError
                            # TODO Is this really the only case where the message should be deleted?
                            if current_message:
                                current_message.delete()
                        write_file_error(job.job_id, job.filename, error_type)
                    mark_job_status(job.job_id, job_status)
            finally:
                GlobalDB.close()
                # Set visibility to 0 so that another attempt can be made to process in SQS immediately,
                # instead of waiting for the timeout window to expire
                for message in messages:
                    try:
                        message.change_visibility(VisibilityTimeout=0)
                    except ClientError:
                        # Deleted messages will throw errors, which is fine because they are handled
                        pass


def get_current_job():
    """Return the job currently stored in flask.g"""
    # The job_id is added to flask.g at the beginning of the validate route. We expect it to be here now, since
    # validate is currently the app's only functional route
    job_id = g.get('job_id', None)
    if job_id:
        sess = GlobalDB.db().session
        return sess.query(Job).filter(Job.job_id == job_id).one_or_none()


if __name__ == "__main__":
    configure_logging()
    run_app()
