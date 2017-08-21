import logging
import csv

from flask import Flask, g, current_app

from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactvalidator.validation_handlers.validationManager import ValidationManager
from dataactcore.aws.sqsHandler import sqs_queue
from dataactcore.interfaces.function_bag import mark_job_status, write_file_error
from dataactcore.utils.responseException import ResponseException
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactcore.models.jobModels import Job


logger = logging.getLogger(__name__)


def create_app():
    return Flask(__name__)


def run_app():
    """Run the application."""
    app = Flask(__name__)

    with app.app_context():
        current_app.debug = CONFIG_SERVICES['debug']
        local = CONFIG_BROKER['local']
        g.is_local = local
        error_report_path = CONFIG_SERVICES['error_report_path']
        current_app.config.from_object(__name__)

        # Future: Override config w/ environment variable, if set
        current_app.config.from_envvar('VALIDATOR_SETTINGS', silent=True)

        queue = sqs_queue()

        logger.info("Starting SQS polling")
        current_message = None
        while True:
            try:
                # Grabs one (or more) messages from the queue
                messages = queue.receive_messages(WaitTimeSeconds=10)
                for message in messages:
                    logger.info("Message received: %s", message.body)
                    current_message = message
                    GlobalDB.db()
                    g.job_id = message.body
                    mark_job_status(g.job_id, "ready")
                    validation_manager = ValidationManager(local, error_report_path)
                    validation_manager.validate_job(g.job_id)

                    # delete from SQS once processed
                    message.delete()
            except ResponseException as e:
                # Handle exceptions explicitly raised during validation.
                logger.error(str(e))

                job = get_current_job()
                if job:
                    if job.filename is not None:
                        # insert file-level error info to the database
                        write_file_error(job.job_id, job.filename, e.errorType, e.extraInfo)
                    if e.errorType != ValidationError.jobError:
                        # job pass prerequisites for validation, but an error
                        # happened somewhere. mark job as 'invalid'
                        mark_job_status(job.job_id, 'invalid')
                        if current_message:
                            if e.errorType in [ValidationError.rowCountError, ValidationError.headerError]:
                                current_message.delete()
            except Exception as e:
                # Handle uncaught exceptions in validation process.
                logger.error(str(e))

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
                    message.change_visibility(VisibilityTimeout=0)


def get_current_job():
    """Return the job currently stored in flask.g"""
    # the job_id is added to flask.g at the beginning of the validate
    # route. we expect it to be here now, since validate is
    # currently the app's only functional route
    job_id = g.get('job_id', None)
    if job_id:
        sess = GlobalDB.db().session
        return sess.query(Job).filter(Job.job_id == job_id).one_or_none()

if __name__ == "__main__":
    configure_logging()
    run_app()
