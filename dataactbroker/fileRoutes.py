from flask import request, session
from dataactbroker.handlers.fileHandler import FileHandler
from dataactbroker.handlers.aws.session import LoginSession
from dataactbroker.permissions import permissions_check
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.jsonResponse import JsonResponse


# Add the file submission route
def add_file_routes(app,CreateCredentials):
    """ Create routes related to file submission for flask app

    """
    CREATE_CREDENTIALS = CreateCredentials
    # Keys for the post route will correspond to the four types of files
    @app.route("/v1/submit_files/", methods = ["POST"])
    @permissions_check
    def submit_files():
        try:
            fileManager = FileHandler(request)
            return fileManager.submit(LoginSession.getName(session),CREATE_CREDENTIALS)
        except Exception as e:
            exc = ResponseException(e.message)
            exc.wrappedException = e
            exc.status = StatusCode.INTERNAL_ERROR
            return JsonResponse.error(exc,exc.status,{})


    @app.route("/v1/finalize_job/", methods = ["POST"])
    @permissions_check
    def finalize_submission():
        try:
            fileManager = FileHandler(request)
            return fileManager.finalize()
        except Exception as e:
            exc = ResponseException(e.message)
            exc.wrappedException = e
            exc.status = StatusCode.INTERNAL_ERROR
            return JsonResponse.error(exc,exc.status,{})

    @app.route("/v1/check_status/", methods = ["POST"])
    @permissions_check
    def check_status():
        try:
            fileManager = FileHandler(request)
            return fileManager.getStatus()
        except Exception as e:
            exc = ResponseException(e.message)
            exc.wrappedException = e
            exc.status = StatusCode.INTERNAL_ERROR
            return JsonResponse.error(exc,exc.status,{})

    @app.route("/v1/submission_error_reports/", methods = ["POST"])
    @permissions_check
    def submission_error_reports():
        try:
            fileManager = FileHandler(request)
            return fileManager.getErrorReportURLsForSubmission()
        except Exception as e:
            exc = ResponseException(e.message)
            exc.wrappedException = e
            exc.status = StatusCode.INTERNAL_ERROR
            return JsonResponse.error(exc,exc.status,{})

    @app.route("/v1/error_metrics/", methods = ["POST"])
    @permissions_check
    def submission_error_metrics():
        try:
            fileManager = FileHandler(request)
            return fileManager.getErrorMetrics()
        except Exception as e:
            exc = ResponseException(e.message)
            exc.wrappedException = e
            exc.status = StatusCode.INTERNAL_ERROR
            return JsonResponse.error(exc,exc.status,{})
