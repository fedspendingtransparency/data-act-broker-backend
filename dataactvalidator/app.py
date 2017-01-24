import logging

import csv
from flask import Flask, request, g

from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import write_file_error, mark_job_status
from dataactcore.logging import configure_logging
from dataactcore.models.jobModels import Job
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.validation_handlers.validationManager import ValidationManager


logger = logging.getLogger(__name__)


def create_app():
    """Create the Flask app."""
    flask_app = Flask(__name__.split('.')[0])
    flask_app.debug = CONFIG_SERVICES['debug']
    local = CONFIG_BROKER['local']
    error_report_path = CONFIG_SERVICES['error_report_path']
    flask_app.config.from_object(__name__)

    # Future: Override config w/ environment variable, if set
    flask_app.config.from_envvar('VALIDATOR_SETTINGS', silent=True)

    @flask_app.teardown_appcontext
    def teardown_appcontext(exception):
        GlobalDB.close()

    @flask_app.before_request
    def before_request():
        GlobalDB.db()

    @flask_app.errorhandler(ResponseException)
    def handle_response_exception(error):
        """Handle exceptions explicitly raised during validation."""
        logger.error(str(error))

        job = get_current_job()
        if job:
            if job.filename is not None:
                # insert file-level error info to the database
                write_file_error(job.job_id, job.filename, error.errorType, error.extraInfo)
            if error.errorType != ValidationError.jobError:
                # job pass prerequisites for validation, but an error
                # happened somewhere. mark job as 'invalid'
                mark_job_status(job.job_id, 'invalid')
        return JsonResponse.error(error, error.status)

    @flask_app.errorhandler(Exception)
    def handle_validation_exception(error):
        """Handle uncaught exceptions in validation process."""
        logger.error(str(error))

        # csv-specific errors get a different job status and response code
        if isinstance(error, ValueError) or isinstance(error, csv.Error):
            job_status, response_code = 'invalid', 400
        else:
            job_status, response_code = 'failed', 500
        job = get_current_job()
        if job:
            if job.filename is not None:
                write_file_error(job.job_id, job.filename, ValidationError.unknownError)
            mark_job_status(job.job_id, job_status)
        return JsonResponse.error(error, response_code)

    @flask_app.route("/", methods=["GET"])
    def test_app():
        """Confirm server running."""
        return "Validator is running"

    @flask_app.route("/validate/", methods=["POST"])
    def validate():
        """Start the validation process."""
        if request.is_json:
            g.job_id = request.json.get('job_id')
        validation_manager = ValidationManager(local, error_report_path)
        return validation_manager.validate_job(request)

    JsonResponse.debugMode = flask_app.debug

    return flask_app


def run_app():
    """Run the application."""
    flask_app = create_app()
    flask_app.run(threaded=True, host=CONFIG_SERVICES['validator_host'], port=CONFIG_SERVICES['validator_port'])


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
elif __name__[0:5] == "uwsgi":
    configure_logging()
    app = create_app()
