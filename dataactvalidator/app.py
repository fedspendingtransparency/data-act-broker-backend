import logging

from flask import Flask, request

from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.interfaceHolder import InterfaceHolder
from dataactcore.logging import configure_logging
from dataactcore.models.jobModels import Job
from dataactcore.models.lookups import JOB_STATUS_DICT
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactvalidator.validation_handlers.validationManager import ValidationManager


_exception_logger = logging.getLogger('deprecated.exception')


def createApp():
    """Create the Flask app."""
    app = Flask(__name__.split('.')[0])
    app.debug = CONFIG_SERVICES['debug']
    local = CONFIG_BROKER['local']
    error_report_path = CONFIG_SERVICES['error_report_path']
    app.config.from_object(__name__)

    # Future: Override config w/ environment variable, if set
    app.config.from_envvar('VALIDATOR_SETTINGS', silent=True)

    @app.teardown_appcontext
    def teardown_appcontext(exception):
        GlobalDB.close()

    @app.before_request
    def before_request():
        GlobalDB.db()

    @app.errorhandler(ResponseException)
    def handle_response_exception(error):
        """Handle exceptions explicitly raised during validation."""
        _exception_logger.exception(str(error))
        return JsonResponse.error(error, error.status)

    @app.errorhandler(Exception)
    def handle_validation_exception(error):
        """Handle uncaught exceptions in validation process."""
        job_id = request.json.get('job_id')

        # if request had a job id, set job to failed status
        if job_id:
            sess = GlobalDB.db().session
            job = sess.query(Job).filter(Job.job_id == job_id).one()
            job.status_id = JOB_STATUS_DICT['failed']
            sess.commit()

        # log failure and return a response
        _exception_logger.exception(str(error))
        return JsonResponse.error(error, 500)

    @app.route("/", methods=["GET"])
    def testApp():
        """Confirm server running."""
        return "Validator is running"

    @app.route("/validate/",methods=["POST"])
    def validate():
        """Start the validation process."""
        interfaces = InterfaceHolder() # Create sessions for this route
        validation_manager = ValidationManager(local, error_report_path)
        return validation_manager.validate_job(request,interfaces)

    JsonResponse.debugMode = app.debug

    return app

def runApp():
    """Run the application."""
    app = createApp()
    app.run(
        threaded=True,
        host=CONFIG_SERVICES['validator_host'],
        port=CONFIG_SERVICES['validator_port']
    )

if __name__ == "__main__":
    configure_logging()
    runApp()
elif __name__[0:5] == "uwsgi":
    configure_logging()
    app = createApp()
