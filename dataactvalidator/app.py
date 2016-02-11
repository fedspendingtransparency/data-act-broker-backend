import os
import inspect
import json
from threading import Thread
from flask import Flask, request, copy_current_request_context
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.responseException import ResponseException
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.validation_handlers.validationManager import ValidationManager
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder

def runApp():
    # Create application
    app = Flask(__name__)
    app.config.from_object(__name__)

    validationManager = ValidationManager()
    # Hold copy of interface objects to limit to a single session for each, this prevents them for being recreated each time
    jobTracker = InterfaceHolder.JOB_TRACKER
    errorDb = InterfaceHolder.ERROR
    stagingDb = InterfaceHolder.STAGING
    validationDb = InterfaceHolder.VALIDATION

    @app.route("/",methods=["GET"])
    def testApp():
        """Confirm server running"""
        # Confirm server running
        return "Validator is running"

    @app.route("/validate_threaded/",methods=["POST"])
    def validate_threaded():
        """Starts the validation process on a new thread"""
        @copy_current_request_context
        def ThreadedFunction (arg) :
                """The new thread"""
                threadedManager = ValidationManager()
                threadedManager.threadedValidateJob(arg)

        jobId = None
        manager = ValidationManager()

        try :
            jobTracker = InterfaceHolder.JOB_TRACKER
        except ResponseException as e:
            return JsonResponse.error(e,e.status,{"table":"cannot connect to job database"})
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            return JsonResponse.error(exc,exc.status,{"table":"cannot connect to job database"})

        try:
            jobId = manager.getJobID(request)
        except ResponseException as e:
            manager.markJob(jobId,jobTracker,"invalid",manager.filename)
            return JsonResponse.error(e,e.status,{"table":""})
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.CLIENT_ERROR,type(e))
            manager.markJob(jobId,jobTracker,"invalid",manager.filename)
            return JsonResponse.error(exc,exc.status,{"table":""})

        try:
            manager.testJobID(jobId)
        except ResponseException as e:
            # Job is not ready to run according to job tracker, do not change status of job in job tracker
            InterfaceHolder.ERROR.writeFileError(jobId,manager.filename,ValidationError.jobError)
            return JsonResponse.error(e,e.status,{"table":""})
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.CLIENT_ERROR,type(e))
            InterfaceHolder.ERROR.writeFileError(jobId,manager.filename,ValidationError.jobError)
            return JsonResponse.error(exc,exc.status,{"table":""})

        thread = Thread(target=ThreadedFunction, args= (jobId,))

        try :
            jobTracker.markStatus(jobId,"running")
        except Exception as e:
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            return JsonResponse.error(exc,exc.status,{"table":"could not start job"})

        thread.start()

        return JsonResponse.create(StatusCode.OK,{"table":"job"+str(jobId)})

    @app.route("/validate/",methods=["POST"])
    def validate():
        """Starts the validation process on the same threads"""
        try:
            return validationManager.validateJob(request)
        except Exception as e:
            # Something went wrong getting the flask request
            open("errorLog","a").write(str(e))
            exc = ResponseException(str(e),StatusCode.INTERNAL_ERROR,type(e))
            return JsonResponse.error(exc,exc.status,{"table":""})


    def getAppConfiguration():
        """Gets the JSON for configuring the validator """
        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        configFile = path + "/validator_configuration.json"
        return json.loads(open(configFile,"r").read())

    config = getAppConfiguration()
    JsonResponse.debugMode = config["rest_trace"]
    app.run(debug=config["server_debug"],threaded=True,host="0.0.0.0",port=int(config["port"]))

if __name__ == '__main__':
    runApp()