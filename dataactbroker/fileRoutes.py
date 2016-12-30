from functools import wraps

from flask import request
from webargs import fields as webargs_fields, validate as webargs_validate
from webargs.flaskparser import parser as webargs_parser, use_kwargs

from dataactbroker.handlers.fileHandler import (
    FileHandler, get_error_metrics, get_status,
    list_submissions as list_submissions_handler,
    narratives_for_submission, submission_report_url, update_narratives
)
from dataactcore.interfaces.function_bag import get_submission_stats
from dataactcore.models.lookups import FILE_TYPE_DICT
from dataactbroker.permissions import requires_login, requires_submission_perms
from dataactcore.models.lookups import FILE_TYPE_DICT_LETTER
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode


# Add the file submission route
def add_file_routes(app, CreateCredentials, isLocal, serverPath):
    """ Create routes related to file submission for flask app

    """
    IS_LOCAL = isLocal
    SERVER_PATH = serverPath

    # Keys for the post route will correspond to the four types of files
    @app.route("/v1/submit_files/", methods=["POST"])
    @requires_login
    def submit_files():
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.submit(CreateCredentials)

    @app.route("/v1/finalize_job/", methods=["POST"])
    @requires_login
    @use_kwargs({'upload_id': webargs_fields.Int(required=True)})
    def finalize_submission(upload_id):
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.finalize(upload_id)

    @app.route("/v1/check_status/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def check_status(submission):
        return get_status(submission)

    @app.route("/v1/submission_error_reports/", methods=["POST"])
    @requires_login
    @use_kwargs({'submission_id': webargs_fields.Int(required=True)})
    def submission_error_reports(submission_id):
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.getErrorReportURLsForSubmission(submission_id)

    @app.route("/v1/submission_warning_reports/", methods=["POST"])
    @requires_login
    @use_kwargs({'submission_id': webargs_fields.Int(required=True)})
    def submission_warning_reports(submission_id):
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.getErrorReportURLsForSubmission(
            submission_id, is_warning=True)

    @app.route("/v1/error_metrics/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def submission_error_metrics(submission):
        return get_error_metrics(submission)

    @app.route("/v1/local_upload/", methods=["POST"])
    @requires_login
    def upload_local_file():
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.uploadFile()

    @app.route("/v1/list_submissions/", methods=["GET"])
    @requires_login
    @use_kwargs({
        'page': webargs_fields.Int(missing=1),
        'limit': webargs_fields.Int(missing=5),
        'certified': webargs_fields.String(
            required=True,
            validate=webargs_validate.OneOf(('mixed', 'true', 'false')))
    })
    def list_submissions(page, limit, certified):
        """ List submission IDs associated with the current user """
        return list_submissions_handler(page, limit, certified)

    @app.route("/v1/get_protected_files/", methods=["GET"])
    @requires_login
    def get_protected_files():
        """ Return signed URLs for all help page files """
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.getProtectedFiles()

    @app.route("/v1/generate_file/", methods=["POST"])
    @convert_to_submission_id
    @use_kwargs({
        'file_type': webargs_fields.String(
            required=True,
            validate=webargs_validate.OneOf(FILE_TYPE_DICT_LETTER.items()))
    })
    def generate_file(submission_id, file_type):
        """ Generate file from external API """
        file_manager = FileHandler(
            request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return file_manager.generate_file(submission_id, file_type)

    @app.route("/v1/generate_detached_file/", methods=["POST"])
    @requires_login
    def generate_detached_file():
        """ Generate a file from external API, independent from a submission """
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.generate_detached_file()

    @app.route("/v1/check_detached_generation_status/", methods=["POST"])
    @requires_login
    def check_detached_generation_status():
        """ Return status of file generation job """
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.check_detached_generation()

    @app.route("/v1/check_generation_status/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def check_generation_status(submission):
        """ Return status of file generation job """
        file_manager = FileHandler(
            request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return file_manager.check_generation(submission)

    @app.route("/v1/complete_generation/<generationId>/", methods=["POST"])
    def complete_generation(generationId):
        fileManager = FileHandler(request, isLocal=IS_LOCAL, serverPath=SERVER_PATH)
        return fileManager.complete_generation(generationId)

    @app.route("/v1/get_obligations/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def get_obligations(submission):
        return JsonResponse.create(
            StatusCode.OK, get_submission_stats(submission.submission_id))

    @app.route("/v1/submission/<int:submission_id>/narrative", methods=['GET'])
    @requires_submission_perms('reader')
    def get_submission_narratives(submission):
        return narratives_for_submission(submission)

    @app.route("/v1/submission/<int:submission_id>/narrative", methods=['POST'])
    @requires_submission_perms('writer')
    def post_submission_narratives(submission):
        json = request.json or {}
        # clean input
        json = {key.upper(): value.strip() for key, value in json.items()
                if isinstance(value, str) and value.strip()}
        return update_narratives(submission, json)

    @app.route("/v1/submission/<int:submission_id>/report_url", methods=['POST'])
    @use_kwargs({
        'warning': webargs_fields.Bool(),
        'file_type': webargs_fields.String(
            required=True,
            validate=webargs_validate.OneOf(FILE_TYPE_DICT.keys())
        ),
        'cross_type': webargs_fields.String(
            validate=webargs_validate.OneOf(FILE_TYPE_DICT.keys()))
    })
    @requires_submission_perms('reader')
    @use_kwargs({
        'warning': webargs_fields.Bool(),
        'file_type': webargs_fields.String(
            required=True,
            validate=webargs_validate.OneOf(FILE_TYPE_DICT.keys())
        ),
        'cross_type': webargs_fields.String(
            validate=webargs_validate.OneOf(FILE_TYPE_DICT.keys()))
    })
    def post_submission_report_url(submission, warning, file_type, cross_type):
        return submission_report_url(
            submission, bool(warning), file_type, cross_type)


def convert_to_submission_id(fn):
    """Decorator which reads the request, looking for a submission key to
    convert into a submission_id parameter. The provided function should have
    a submission_id parameter as its first argument."""
    @wraps(fn)
    @requires_login     # check login before checking submission_id
    def wrapped(*args, **kwargs):
        req_args = webargs_parser.parse({
            'submission': webargs_fields.Int(),
            'submission_id': webargs_fields.Int()
        })
        submission_id = req_args.get('submission',
                                     req_args.get('submission_id'))
        if submission_id is None:
            raise ResponseException(
                "submission_id is required", StatusCode.CLIENT_ERROR)
        return fn(submission_id, *args, **kwargs)
    return wrapped
