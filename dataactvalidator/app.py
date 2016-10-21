import sys
import traceback
from threading import Thread
from flask import Flask, request, copy_current_request_context
from dataactcore.interfaces.db import GlobalDB
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.cloudLogger import CloudLogger
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.validation_handlers.validationManager import ValidationManager
from dataactcore.interfaces.interfaceHolder import InterfaceHolder


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

        validationManager = ValidationManager(local, error_report_path)

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

        @app.route("/validate_threaded/", methods=["POST"])
        def validate_threaded():
            """Start the validation process on a new thread."""
            @copy_current_request_context
            def ThreadedFunction(arg):
                """The new thread."""
                threadedManager = ValidationManager(local, error_report_path)
                threadedManager.threadedValidateJob(arg)

            try:
                interfaces = InterfaceHolder()
                jobTracker = interfaces.jobDb
            except ResponseException as e:
                open("errorLog","a").write(str(e) + "\n")
                return JsonResponse.error(e,e.status)
            except Exception as e:
                open("errorLog","a").write(str(e) + "\n")
                exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
                return JsonResponse.error(exc,exc.status)

            jobId = None
            manager = ValidationManager(local, error_report_path)

            try:
                jobId = manager.getJobID(request)
            except ResponseException as e:
                manager.markJob(jobId,jobTracker,"invalid",interfaces.errorDb,manager.filename)
                CloudLogger.logError(str(e),e,traceback.extract_tb(sys.exc_info()[2]))
                return JsonResponse.error(e,e.status)
            except Exception as e:
                exc = ResponseException(str(e),StatusCode.CLIENT_ERROR,type(e))
                manager.markJob(jobId,jobTracker,"invalid",interfaces.errorDb,manager.filename)
                CloudLogger.logError(str(e),exc,traceback.extract_tb(sys.exc_info()[2]))
                return JsonResponse.error(exc,exc.status)

            try:
                manager.testJobID(jobId,interfaces)
            except ResponseException as e:
                open("errorLog","a").write(str(e) + "\n")
                # Job is not ready to run according to job tracker, do not change status of job in job tracker
                interfaces.errorDb.writeFileError(jobId,manager.filename,ValidationError.jobError)
                return JsonResponse.error(e,e.status)
            except Exception as e:
                open("errorLog","a").write(str(e) + "\n")
                exc = ResponseException(str(e),StatusCode.CLIENT_ERROR,type(e))
                interfaces.errorDb.writeFileError(jobId,manager.filename,ValidationError.jobError)
                return JsonResponse.error(exc,exc.status)

            thread = Thread(target=ThreadedFunction, args= (jobId,))

            try :
                jobTracker.markJobStatus(jobId,"running")
            except Exception as e:
                open("errorLog","a").write(str(e) + "\n")
                exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
                return JsonResponse.error(exc,exc.status)

            interfaces.close()
            thread.start()

            return JsonResponse.create(StatusCode.OK, {"message":"Validation complete"})

        @app.route("/validate/",methods=["POST"])
        def validate():
            """Start the validation process on the same threads."""
            interfaces = InterfaceHolder() # Create sessions for this route
            try:
                return validationManager.validateJob(request,interfaces)
            except Exception as e:
                # Something went wrong getting the flask request
                open("errorLog","a").write(str(e) + "\n")
                exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
                return JsonResponse.error(exc,exc.status)
            finally:
                interfaces.close()

        JsonResponse.debugMode = CONFIG_SERVICES['rest_trace']

        return app

    except Exception as e:
        trace = traceback.extract_tb(sys.exc_info()[2], 10)
        CloudLogger.logError('Validator App Level Error: ', e, trace)
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
    runApp()
elif __name__[0:5] == "uwsgi":
    app = createApp()
