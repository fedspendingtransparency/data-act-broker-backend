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
    get_revalidation_threshold)
from dataactbroker.decorators import convert_to_submission_id
from dataactbroker.permissions import (requires_login, requires_submission_perms, requires_agency_perms,
                                       requires_sub_agency_perms)

from dataactcore.interfaces.function_bag import get_fabs_meta
from dataactcore.models.lookups import FILE_TYPE_DICT, FILE_TYPE_DICT_LETTER
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.statusCode import StatusCode

from dataactbroker.handlers.dashboard_handler import historic_dabs_warning_summary


# Add the agency data dashboard routes
def add_dashboard_routes(app):
    """ Create routes related to agency data dashboard for flask app """

    @app.route("/v1/historic_dabs_summary/", methods=["POST"])
    @requires_login
    @use_kwargs({
        'filters': webargs_fields.Dict(keys=webargs_fields.String(), missing={})
    })
    def historic_dabs_summary(**kwargs):
        filters = kwargs.get('filters')
        return historic_dabs_warning_summary(filters)

