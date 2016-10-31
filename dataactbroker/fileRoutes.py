from flask import request, session
from dataactbroker.handlers.fileHandler import FileHandler
from dataactbroker.handlers.accountHandler import AccountHandler
from dataactbroker.permissions import permissions_check
from dataactbroker.routeUtils import RouteUtils
from dataactbroker.handlers.aws.session import LoginSession

# Add the file submission route
def add_file_routes(app,CreateCredentials,isLocal,serverPath,bcrypt):
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
        return RouteUtils.run_instance_function(fileManager, fileManager.submit, LoginSession.getName(session), RouteUtils.CREATE_CREDENTIALS)

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

    @app.route("/v1/submission_warning_reports/", methods = ["POST"])
    @permissions_check
    def submission_warning_reports():
        fileManager = FileHandler(request,isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return RouteUtils.run_instance_function(fileManager, fileManager.getErrorReportURLsForSubmission, True)

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

    @app.route("/v1/list_submissions/", methods = ["GET"])
    @permissions_check
    def list_submissions():
        """ List submission IDs associated with the current user """
        filter_by = request.args.get('filter_by')
        filter_by = filter_by.lower() if filter_by is not None else filter_by
        accountManager = AccountHandler(request,bcrypt = bcrypt)

        if filter_by == 'agency':
            return RouteUtils.run_instance_function(accountManager, accountManager.listSubmissionsByCurrentUserAgency)
        return RouteUtils.run_instance_function(accountManager, accountManager.listSubmissionsByCurrentUser)

    @app.route("/v1/get_protected_files/", methods=["GET"])
    @permissions_check
    def get_protected_files():
        """ Return signed URLs for all help page files """
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return RouteUtils.run_instance_function(fileManager, fileManager.getProtectedFiles)

    @app.route("/v1/generate_file/", methods=["POST"])
    @permissions_check
    def generate_file():
        """ Generate file from external API """
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return RouteUtils.run_instance_function(fileManager, fileManager.generateFile)

    @app.route("/v1/check_generation_status/", methods=["POST"])
    @permissions_check
    def check_generation_status():
        """ Return status of file generation job """
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return RouteUtils.run_instance_function(fileManager, fileManager.checkGeneration)

    @app.route("/v1/complete_generation/<generationId>/", methods=["POST"])
    def complete_generation(generationId):
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return RouteUtils.run_instance_function(fileManager, fileManager.completeGeneration, generationId)

    @app.route("/v1/get_obligations/", methods = ["POST"])
    @permissions_check
    def get_obligations():
        fileManager = FileHandler(request,isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return RouteUtils.run_instance_function(fileManager, fileManager.getObligations)