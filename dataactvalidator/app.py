import logging

from flask import Flask, g, current_app

from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactvalidator.validation_handlers.validationManager import ValidationManager
from dataactcore.aws.sqsHandler import sqs_queue


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

        @current_app.teardown_appcontext
        def teardown_appcontext(exception):
            GlobalDB.close()

        @current_app.before_request
        def before_request():
            GlobalDB.db()

        queue = sqs_queue()

        logger.info("Starting SQS polling")
        while True:
            # Grabs one (or more) messages from the queue
            messages = queue.receive_messages(WaitTimeSeconds=10)
            for message in messages:
                validation_manager = ValidationManager(local, error_report_path)
                validation_manager.validate_job(message.body)
                logger.info("Message received: %s", message.body)

                # delete from SQS once processed
                message.delete()

if __name__ == "__main__":
    configure_logging()
    run_app()
