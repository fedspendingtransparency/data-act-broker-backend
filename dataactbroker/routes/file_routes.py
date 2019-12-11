from flask import request
from webargs import fields as webargs_fields, validate as webargs_validate
from webargs.flaskparser import use_kwargs

from dataactbroker.handlers.fileHandler import (
    FileHandler, get_error_metrics, get_status, list_submissions as list_submissions_handler, get_upload_file_url,
    get_detached_upload_file_url, get_submission_comments, submission_report_url, update_submission_comments,
    list_certifications, file_history_url, get_comments_file)
from dataactbroker.handlers.submission_handler import (
    delete_all_submission_data, get_submission_stats, list_windows, check_current_submission_page,
    certify_dabs_submission, find_existing_submissions_in_period, get_submission_metadata, get_submission_data,
    get_revalidation_threshold, get_latest_certification_period)
from dataactbroker.decorators import convert_to_submission_id
from dataactbroker.permissions import (requires_login, requires_submission_perms, requires_agency_perms,
                                       requires_sub_agency_perms)

from dataactcore.interfaces.function_bag import get_fabs_meta
from dataactcore.models.lookups import FILE_TYPE_DICT, FILE_TYPE_DICT_LETTER
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.statusCode import StatusCode


# Add the file submission route
def add_file_routes(app, is_local, server_path):
    """ Create routes related to file submission for flask app """

    # Keys for the post route will correspond to the four types of files
    @app.route("/v1/upload_dabs_files/", methods=["POST"])
    @requires_agency_perms('writer')
    def upload_dabs_files():
        if "multipart/form-data" not in request.headers['Content-Type']:
            return JsonResponse.error(ValueError("Request must be a multipart/form-data type"), StatusCode.CLIENT_ERROR)
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.validate_upload_dabs_files()

    @app.route("/v1/check_status/", methods=["GET"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    @use_kwargs({'type': webargs_fields.String(missing='')})
    def check_status(submission, **kwargs):
        type = kwargs.get('type')
        return get_status(submission, type)

    @app.route("/v1/submission_metadata/", methods=["GET"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def submission_metadata(submission):
        return JsonResponse.create(StatusCode.OK, get_submission_metadata(submission))

    @app.route("/v1/submission_data/", methods=["GET"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    @use_kwargs({'type': webargs_fields.String(missing='')})
    def submission_data(submission, **kwargs):
        type = kwargs.get('type')
        return get_submission_data(submission, type)

    @app.route("/v1/revalidation_threshold/", methods=["GET"])
    @requires_login
    def revalidation_threshold():
        return JsonResponse.create(StatusCode.OK, get_revalidation_threshold())

    @app.route("/v1/latest_certification_period/", methods=["GET"])
    @requires_login
    def latest_certification_period():
        return JsonResponse.create(StatusCode.OK, get_latest_certification_period())

    @app.route("/v1/window/", methods=["GET"])
    def window():
        return list_windows()

    @app.route("/v1/error_metrics/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def submission_error_metrics(submission):
        return get_error_metrics(submission)

    @app.route("/v1/list_submissions/", methods=["POST"])
    @requires_login
    @use_kwargs({
        'page': webargs_fields.Int(missing=1),
        'limit': webargs_fields.Int(missing=5),
        'certified': webargs_fields.String(
            required=True,
            validate=webargs_validate.OneOf(('mixed', 'true', 'false'))),
        'sort': webargs_fields.String(missing='modified'),
        'order': webargs_fields.String(missing='desc'),
        'fabs': webargs_fields.Bool(missing=False),
        'filters': webargs_fields.Dict(keys=webargs_fields.String(), missing={})
    })
    def list_submissions(certified, **kwargs):
        """ List submission IDs associated with the current user """
        page = kwargs.get('page')
        limit = kwargs.get('limit')
        sort = kwargs.get('sort')
        order = kwargs.get('order')
        fabs = kwargs.get('fabs')
        filters = kwargs.get('filters')
        return list_submissions_handler(page, limit, certified, sort, order, fabs, filters)

    @app.route("/v1/list_certifications/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def submission_list_certifications(submission):
        """ List all certifications for a specific submission """
        return list_certifications(submission)

    @app.route("/v1/get_certified_file/", methods=["POST"])
    @use_kwargs({
        'submission_id': webargs_fields.Int(required=True),
        'certified_files_history_id': webargs_fields.Int(required=True),
        'is_warning': webargs_fields.Bool(missing=False)
    })
    @requires_submission_perms('reader')
    def get_certified_file(submission, certified_files_history_id, **kwargs):
        """ Get the signed URL for the specified file history """
        is_warning = kwargs.get('is_warning')
        return file_history_url(submission, certified_files_history_id, is_warning, is_local)

    @app.route("/v1/check_current_page/", methods=["GET"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def check_current_page(submission):
        return check_current_submission_page(submission)

    @app.route("/v1/get_fabs_meta/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def get_fabs_metadata(submission):
        """ Return metadata of FABS submission """
        return JsonResponse.create(StatusCode.OK, get_fabs_meta(submission.submission_id))

    @app.route("/v1/upload_fabs_file/", methods=["POST"])
    @requires_sub_agency_perms('editfabs')
    def upload_fabs_file():
        if "multipart/form-data" not in request.headers['Content-Type']:
            return JsonResponse.error(ValueError("Request must be a multipart/form-data type"), StatusCode.CLIENT_ERROR)
        params = RequestDictionary.derive(request)
        fabs = params.get('_files', {}).get('fabs', None)
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.upload_fabs_file(fabs)

    @app.route("/v1/publish_fabs_file/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('fabs', check_owner=False)
    def publish_fabs_file(submission):
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return file_manager.publish_fabs_submission(submission)

    @app.route("/v1/get_obligations/", methods=["GET"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def get_obligations(submission):
        return JsonResponse.create(StatusCode.OK, get_submission_stats(submission.submission_id))

    # TODO: Deprecated, remove at proper time
    @app.route("/v1/submission/<int:submission_id>/narrative", methods=['GET'])
    @requires_submission_perms('reader')
    def get_submission_narratives(submission):
        return get_submission_comments(submission)

    # TODO: Deprecated, remove at proper time
    @app.route("/v1/submission/<int:submission_id>/narrative", methods=['POST'])
    @requires_submission_perms('writer')
    def post_submission_narratives(submission):
        return update_submission_comments(submission, request.json, is_local)

    @app.route("/v1/get_submission_comments/", methods=['GET'])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def get_sub_comments(submission):
        return get_submission_comments(submission)

    @app.route("/v1/update_submission_comments/", methods=['POST'])
    @convert_to_submission_id
    @requires_submission_perms('writer')
    def update_sub_comments(submission):
        return update_submission_comments(submission, request.json, is_local)

    @app.route("/v1/get_comments_file/", methods=['GET'])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    def get_submission_comments_file(submission):
        return get_comments_file(submission, is_local)

    @app.route("/v1/submission/<int:submission_id>/report_url/", methods=['GET'])
    @requires_submission_perms('reader')
    @use_kwargs({
        'file_type': webargs_fields.String(
            required=True,
            validate=webargs_validate.OneOf(FILE_TYPE_DICT.keys() - {'executive_compensation', 'sub_award'})
        ),
        'warning': webargs_fields.Bool(),
        'cross_type': webargs_fields.String(validate=webargs_validate.OneOf(['program_activity', 'award_financial',
                                                                             'award_procurement', 'award']))
    })
    def post_submission_report_url(submission, file_type, **kwargs):
        warning = kwargs.get('warning')
        cross_type = kwargs.get('cross_type')
        return submission_report_url(submission, bool(warning), file_type, cross_type)

    @app.route("/v1/get_file_url/", methods=['GET'])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    @use_kwargs({
        'file_type': webargs_fields.String(
            required=True,
            validate=webargs_validate.OneOf(FILE_TYPE_DICT_LETTER.values())
        )
    })
    def get_file_url(submission, file_type):
        return get_upload_file_url(submission, file_type)

    @app.route("/v1/get_detached_file_url/", methods=['GET'])
    @requires_login
    @use_kwargs({
        'job_id': webargs_fields.Int(required=True)
    })
    def get_detached_file_url(job_id):
        return get_detached_upload_file_url(job_id)

    @app.route("/v1/delete_submission/", methods=['POST'])
    @convert_to_submission_id
    @requires_submission_perms('writer', check_fabs='editfabs')
    def delete_submission(submission):
        """ Deletes all data associated with the specified submission
            NOTE: THERE IS NO WAY TO UNDO THIS
        """
        return delete_all_submission_data(submission)

    @app.route("/v1/check_year_quarter/", methods=["GET"])
    @requires_login
    @use_kwargs({'reporting_fiscal_year': webargs_fields.String(required=True),
                 'reporting_fiscal_period': webargs_fields.String(required=True),
                 'cgac_code': webargs_fields.String(),
                 'frec_code': webargs_fields.String()})
    def check_year_and_quarter(reporting_fiscal_year, reporting_fiscal_period, **kwargs):
        """ Check if cgac (or frec) code, year, and quarter already has a published submission """
        cgac_code = kwargs.get('cgac_code')
        frec_code = kwargs.get('frec_code')
        return find_existing_submissions_in_period(cgac_code, frec_code, reporting_fiscal_year, reporting_fiscal_period)

    @app.route("/v1/certify_submission/", methods=['POST'])
    @convert_to_submission_id
    @requires_submission_perms('submitter', check_owner=False)
    def certify_submission(submission):
        file_manager = FileHandler(request, is_local=is_local, server_path=server_path)
        return certify_dabs_submission(submission, file_manager)

    @app.route("/v1/restart_validation/", methods=['POST'])
    @convert_to_submission_id
    @requires_submission_perms('writer', check_fabs='editfabs')
    @use_kwargs({'d2_submission': webargs_fields.Bool(missing=False)})
    def restart_validation(submission, **kwargs):
        d2_submission = kwargs.get('d2_submission')
        return FileHandler.restart_validation(submission, d2_submission)
