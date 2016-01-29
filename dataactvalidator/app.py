
import os
import inspect

import sys
import flask
from threading import Thread
from flask import Flask, request, make_response, session, g, redirect, url_for, \
     abort, render_template, flash ,session, Response, copy_current_request_context
import json

from dataactvalidator.validation_handlers.validationManager import ValidationManager
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.responseException import ResponseException
from csv import Error
from dataactvalidator.interfaces.jobTrackerInterface import JobTrackerInterface
from dataactvalidator.interfaces.errorInterface import ErrorInterface
from dataactvalidator.interfaces.stagingInterface import StagingInterface
from dataactvalidator.interfaces.validationInterface import ValidationInterface
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder

def runApp():
    debugFlag = True

    # Create application
    app = Flask(__name__)
    app.config.from_object(__name__)

    validationManager = ValidationManager()
    # Hold copy of interface objects to limit to a single session for each
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
        def markJob(job,jobTracker,status) :
            """helper function to mark status without throwing errors"""
            try :
                jobTracker.markStatus(jobId,status)
            except Exception as e:
               pass

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
            exc = ResponseException(e.message)
            exc.wrappedException = e
            exc.status = StatusCode.CLIENT_ERROR
            return JsonResponse.error(exc,exc.status,{"table":"cannot connect to job database"})
        except Exception as e:
            exc = ResponseException(e.message)
            exc.wrappedException = e
            exc.status = StatusCode.INTERNAL_ERROR
            return JsonResponse.error(exc,exc.status,{"table":"cannot connect to job database"})

        try:
            jobId = manager.getJobID(flask.request)
        except ResponseException as e:
            exc = ResponseException(e.message)
            exc.wrappedException = e
            exc.status = StatusCode.CLIENT_ERROR
            manager.markJob(jobId,jobTracker,"invalid")
            errorHandler = InterfaceHolder.ERROR
            errorHandler.writeFileError(jobId,manager.filename,ValidationError.unknownError)
            return JsonResponse.error(exc,exc.status,{"table":""})
        except Exception as e:
            exc = ResponseException(e.message)
            exc.wrappedException = e
            exc.status = StatusCode.CLIENT_ERROR
            manager.markJob(jobId,jobTracker,"invalid")
            errorHandler = InterfaceHolder.ERROR
            errorHandler.writeFileError(jobId,manager.filename,ValidationError.unknownError)
            return JsonResponse.error(exc,exc.status,{"table":""})

        try:
            manager.testJobID(jobId)
        except ResponseException as e:
            exc = ResponseException(e.message)
            exc.wrappedException = e
            exc.status = StatusCode.CLIENT_ERROR
            errorHandler = InterfaceHolder.ERROR
            errorHandler.writeFileError(jobId,manager.filename,ValidationError.jobError)
            return JsonResponse.error(exc,exc.status,{"table":""})
        except Exception as e:
            exc = ResponseException(e.message)
            exc.wrappedException = e
            exc.status = StatusCode.CLIENT_ERROR
            errorHandler = InterfaceHolder.ERROR
            errorHandler.writeFileError(jobId,manager.filename,ValidationError.jobError)
            return JsonResponse.error(exc,exc.status,{"table":""})

        thread = Thread(target=ThreadedFunction, args= (jobId,))

        try :
            jobTracker.markStatus(jobId,"running")
        except Exception as e:
            exc = ResponseException(e.message)
            exc.wrappedException = e
            exc.status = StatusCode.INTERNAL_ERROR
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
            open("errorLog","a").write(e.message)
            exc = ResponseException("Internal exception")
            exc.status = StatusCode.INTERNAL_ERROR
            exc.wrappedException = e
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