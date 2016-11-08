import logging

from flask import Flask, request

from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.interfaceHolder import InterfaceHolder
from dataactcore.logging import configure_logging
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactvalidator.validation_handlers.validationManager import ValidationManager


_exception_logger = logging.getLogger('deprecated.exception')


def createApp():
    """Create the Flask app."""
    try:
        app = Flask(__name__.split('.')[0])
        app.debug = CONFIG_SERVICES['server_debug']
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

        @app.route("/", methods=["GET"])
        def testApp():
            """Confirm server running."""
            return "Validator is running"

        @app.route("/validate/",methods=["POST"])
        def validate():
            """Start the validation process on the same threads."""
            interfaces = InterfaceHolder() # Create sessions for this route
            try:
                validationManager = ValidationManager(local, error_report_path)
                return validationManager.validate_job(request, interfaces)
            except Exception as e:
                # Something went wrong getting the flask request
                open("errorLog","a").write(str(e) + "\n")
                exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
                return JsonResponse.error(exc,exc.status)
            finally:
                interfaces.close()

        JsonResponse.debugMode = CONFIG_SERVICES['rest_trace']

        return app
    except:
        _exception_logger.exception('Validator App Level Error: ')
        raise

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
