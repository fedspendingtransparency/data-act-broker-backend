from flask import request
from dataactbroker.handlers.fileHandler import FileHandler
from dataactbroker.permissions import permissions_check
from dataactbroker.routeUtils import RouteUtils

# Add the file submission route
def add_file_routes(app,CreateCredentials,isLocal,serverPath):
    """ Create routes related to file submission for flask app

    """
    RouteUtils.CREATE_CREDENTIALS = CreateCredentials
    IS_LOCAL =isLocal
    SERVER_PATH  = serverPath
    # Keys for the post route will correspond to the four types of files
    @app.route("/v1/submit_files/", methods = ["POST"])
    @permissions_check
    def submit_files():
        fileManager = FileHandler(request,isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return RouteUtils.run_instance_function(fileManager, fileManager.submit, getUser=True,getCredentials=True)

    @app.route("/v1/finalize_job/", methods = ["POST"])
    @permissions_check
    def finalize_submission():
        fileManager = FileHandler(request,isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return RouteUtils.run_instance_function(fileManager, fileManager.finalize)

    @app.route("/v1/check_status/", methods = ["POST"])
    @permissions_check
    def check_status():
        fileManager = FileHandler(request,isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return RouteUtils.run_instance_function(fileManager, fileManager.getStatus)

    @app.route("/v1/submission_error_reports/", methods = ["POST"])
    @permissions_check
    def submission_error_reports():
        fileManager = FileHandler(request,isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return RouteUtils.run_instance_function(fileManager, fileManager.getErrorReportURLsForSubmission)

    @app.route("/v1/error_metrics/", methods = ["POST"])
    @permissions_check
    def submission_error_metrics():
        fileManager = FileHandler(request,isLocal=IS_LOCAL ,serverPath=SERVER_PATH)
        return RouteUtils.run_instance_function(fileManager, fileManager.getErrorMetrics)

    @app.route("/v1/local_upload/", methods = ["POST"])
    @permissions_check
    def upload_local_file():
        fileManager = FileHandler(request,isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return RouteUtils.run_instance_function(fileManager, fileManager.uploadFile)
