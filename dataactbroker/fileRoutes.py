from flask import request, session
from dataactbroker.handlers.fileHandler import (
    FileHandler, narratives_for_submission, update_narratives)
from dataactbroker.permissions import permissions_check, requires_login
from dataactbroker.handlers.aws.session import LoginSession
from dataactbroker.exceptions.invalid_usage import InvalidUsage


# Add the file submission route
def add_file_routes(app,CreateCredentials,isLocal,serverPath,bcrypt):
    """ Create routes related to file submission for flask app

    """
    IS_LOCAL =isLocal
    SERVER_PATH  = serverPath
    # Keys for the post route will correspond to the four types of files
    @app.route("/v1/submit_files/", methods = ["POST"])
    @permissions_check(permission="writer")
    def submit_files():
        fileManager = FileHandler(request,isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.submit(LoginSession.getName(session), CreateCredentials)

    @app.route("/v1/finalize_job/", methods = ["POST"])
    @permissions_check(permission="writer")
    def finalize_submission():
        fileManager = FileHandler(request,isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.finalize()

    @app.route("/v1/check_status/", methods = ["POST"])
    @requires_login
    def check_status():
        fileManager = FileHandler(request,isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.getStatus()

    @app.route("/v1/submission_error_reports/", methods = ["POST"])
    @requires_login
    def submission_error_reports():
        fileManager = FileHandler(request,isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.getErrorReportURLsForSubmission()

    @app.route("/v1/submission_warning_reports/", methods = ["POST"])
    @requires_login
    def submission_warning_reports():
        fileManager = FileHandler(request,isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.getErrorReportURLsForSubmission(True)

    @app.route("/v1/error_metrics/", methods = ["POST"])
    @requires_login
    def submission_error_metrics():
        fileManager = FileHandler(request,isLocal=IS_LOCAL ,serverPath=SERVER_PATH)
        return fileManager.get_error_metrics()

    @app.route("/v1/local_upload/", methods = ["POST"])
    @requires_login
    def upload_local_file():
        fileManager = FileHandler(request,isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.uploadFile()

    @app.route("/v1/list_submissions/", methods = ["GET"])
    @requires_login
    def list_submissions():
        """ List submission IDs associated with the current user """

        page = request.args.get('page')
        limit = request.args.get('limit')
        certified = request.args.get('certified')

        # convert params and type check
        try:
            page = int(page) if page is not None else 1
        except:
            raise InvalidUsage("Incorrect type specified for 'page'. Please enter a positive number.")

        try:
            limit = int(limit) if limit is not None else 5
        except:
            raise InvalidUsage("Incorrect type specified for 'limit'. Please enter a positive number.")

        if certified is not None:
            certified = certified.lower()
        else:
            raise InvalidUsage("Missing required parameter 'certified'")
        # If certified is none, get all submissions without filtering
        if certified is not None and certified not in ['mixed', 'true', 'false']:
            raise InvalidUsage("Incorrect value specified for the 'certified' parameter")

        file_manager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return file_manager.list_submissions(page, limit, certified)

    @app.route("/v1/get_protected_files/", methods=["GET"])
    @requires_login
    def get_protected_files():
        """ Return signed URLs for all help page files """
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.getProtectedFiles()

    @app.route("/v1/generate_file/", methods=["POST"])
    @permissions_check(permission="writer")
    def generate_file():
        """ Generate file from external API """
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.generateFile()

    @app.route("/v1/generate_detached_file/", methods=["POST"])
    @permissions_check(permission="reader")
    def generate_detached_file():
        """ Generate a file from external API, independent from a submission """
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.generate_detached_file()

    @app.route("/v1/check_detached_generation_status/", methods=["POST"])
    @permissions_check
    def check_detached_generation_status():
        """ Return status of file generation job """
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.check_detached_generation()

    @app.route("/v1/check_generation_status/", methods=["POST"])
    @requires_login
    def check_generation_status():
        """ Return status of file generation job """
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.checkGeneration()

    @app.route("/v1/complete_generation/<generationId>/", methods=["POST"])
    def complete_generation(generationId):
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.complete_generation(generationId)

    @app.route("/v1/get_obligations/", methods = ["POST"])
    @requires_login
    def get_obligations():
        fileManager = FileHandler(request,isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.getObligations()

    @app.route("/v1/sign_submission_file", methods = ["POST"])
    @requires_login
    def sign_submission_file():
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.get_signed_url_for_submission_file()

    @app.route("/v1/submission/<int:submission_id>/narrative", methods=['GET'])
    @permissions_check(permission='reader')
    def get_submission_narratives(submission_id):
        return narratives_for_submission(int(submission_id))

    @app.route("/v1/submission/<int:submission_id>/narrative", methods=['POST'])
    @permissions_check(permission='writer')
    def post_submission_narratives(submission_id):
        json = request.json or {}
        # clean input
        json = {key.upper():value.strip() for key, value in json.items()
                if isinstance(value, str) and value.strip()}
        return update_narratives(int(submission_id), json)
