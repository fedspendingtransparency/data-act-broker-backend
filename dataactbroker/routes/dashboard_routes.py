from webargs import fields as webargs_fields, validate as webargs_validate
from webargs.flaskparser import use_kwargs

from dataactbroker.decorators import convert_to_submission_id
from dataactbroker.permissions import requires_login, requires_submission_perms

from dataactbroker.handlers.dashboard_handler import (historic_dabs_warning_summary, historic_dabs_warning_table,
                                                      list_rule_labels, historic_dabs_warning_graphs,
                                                      active_submission_overview, active_submission_table,
                                                      get_impact_counts, get_significance_counts)
from dataactbroker.helpers.dashboard_helper import FILE_TYPES


# Add the agency data dashboard routes
def add_dashboard_routes(app):
    """ Create routes related to agency data dashboard for flask app """

    @app.route('/v1/get_rule_labels/', methods=['POST'])
    @use_kwargs({
        'files': webargs_fields.List(webargs_fields.String(), required=True),
        'fabs': webargs_fields.Bool(missing=False),
        'error_level': webargs_fields.String(validate=webargs_validate.
                                             OneOf(['warning', 'error', 'mixed'],
                                                   error='Must be either warning, error, or mixed'),
                                             missing='warning')
    })
    def get_rule_labels(**kwargs):
        """ Returns the rule labels based on the filters provided """
        files = kwargs.get('files')
        fabs = kwargs.get('fabs')
        error_level = kwargs.get('error_level')
        return list_rule_labels(files, error_level, fabs)

    @app.route("/v1/historic_dabs_summary/", methods=["POST"])
    @requires_login
    @use_kwargs({
        'filters': webargs_fields.Dict(keys=webargs_fields.String(), missing={})
    })
    def historic_dabs_summary(**kwargs):
        """ Returns the historic DABS summaries based on the filters provided """
        filters = kwargs.get('filters')
        return historic_dabs_warning_summary(filters)

    @app.route("/v1/historic_dabs_graphs/", methods=["POST"])
    @requires_login
    @use_kwargs({
        'filters': webargs_fields.Dict(keys=webargs_fields.String(), missing={})
    })
    def historic_dabs_graphs(**kwargs):
        """ Returns the historic DABS graphs based on the filters provided """
        filters = kwargs.get('filters')
        return historic_dabs_warning_graphs(filters)

    @app.route("/v1/historic_dabs_table/", methods=["POST"])
    @requires_login
    @use_kwargs({
        'page': webargs_fields.Int(missing=1),
        'limit': webargs_fields.Int(missing=5),
        'sort': webargs_fields.String(missing='period'),
        'order': webargs_fields.String(missing='desc'),
        'filters': webargs_fields.Dict(keys=webargs_fields.String(), required=True)
    })
    def historic_dabs_table(**kwargs):
        """ List warning metadata for selected  """
        page = kwargs.get('page')
        limit = kwargs.get('limit')
        sort = kwargs.get('sort')
        order = kwargs.get('order')
        filters = kwargs.get('filters')
        return historic_dabs_warning_table(filters, page, limit, sort, order)

    @app.route("/v1/active_submission_overview/", methods=["GET"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    @use_kwargs({
        'file': webargs_fields.String(validate=webargs_validate.
                                      OneOf(['A', 'B', 'C', 'cross-AB', 'cross-BC', 'cross-CD1', 'cross-CD2'],
                                            error='Must be A, B, C, cross-AB, cross-BC, cross-CD1, or cross-CD2'),
                                      required=True),
        'error_level': webargs_fields.String(validate=webargs_validate.
                                             OneOf(['warning', 'error', 'mixed'],
                                                   error='Must be either warning, error, or mixed'),
                                             missing='warning')
    })
    def get_active_submission_overview(submission, file, **kwargs):
        """ Returns an overview of the requested submission for the active dashboard """
        error_level = kwargs.get('error_level')
        return active_submission_overview(submission, file, error_level)

    @app.route("/v1/get_impact_counts/", methods=["GET"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    @use_kwargs({
        'file': webargs_fields.String(validate=webargs_validate.
                                      OneOf(['A', 'B', 'C', 'cross-AB', 'cross-BC', 'cross-CD1', 'cross-CD2'],
                                            error='Must be A, B, C, cross-AB, cross-BC, cross-CD1, or cross-CD2'),
                                      required=True),
        'error_level': webargs_fields.String(validate=webargs_validate.
                                             OneOf(['warning', 'error', 'mixed'],
                                                   error='Must be either warning, error, or mixed'),
                                             missing='warning')
    })
    def impact_counts(submission, file, **kwargs):
        """ Returns the impact counts of the requested submission for the active dashboard """
        error_level = kwargs.get('error_level')
        return get_impact_counts(submission, file, error_level)

    @app.route("/v1/get_significance_counts/", methods=["GET"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    @use_kwargs({
        'file': webargs_fields.String(validate=webargs_validate.
                                      OneOf(['A', 'B', 'C', 'cross-AB', 'cross-BC', 'cross-CD1', 'cross-CD2'],
                                            error='Must be A, B, C, cross-AB, cross-BC, cross-CD1, or cross-CD2'),
                                      required=True),
        'error_level': webargs_fields.String(validate=webargs_validate.
                                             OneOf(['warning', 'error', 'mixed'],
                                                   error='Must be either warning, error, or mixed'),
                                             missing='warning')
    })
    def significance_counts(submission, file, **kwargs):
        """ Returns the significance counts of the requested submission for the active dashboard """
        error_level = kwargs.get('error_level')
        return get_significance_counts(submission, file, error_level)

    @app.route("/v1/active_submission_table/", methods=["GET"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    @use_kwargs({
        'file': webargs_fields.String(validate=webargs_validate.
                                      OneOf(FILE_TYPES,
                                            error='Must be one of the following: {}'.format(', '.join(FILE_TYPES))),
                                      required=True),
        'error_level': webargs_fields.String(validate=webargs_validate.
                                             OneOf(['warning', 'error', 'mixed'],
                                                   error='Must be either warning, error, or mixed'),
                                             missing='warning'),
        'page': webargs_fields.Int(missing=1),
        'limit': webargs_fields.Int(missing=5),
        'sort': webargs_fields.String(missing='significance'),
        'order': webargs_fields.String(missing='desc')
    })
    def get_active_submission_table(submission, file, **kwargs):
        """ Returns an overview of the requested submission for the active dashboard """
        error_level = kwargs.get('error_level')
        page = kwargs.get('page')
        limit = kwargs.get('limit')
        sort = kwargs.get('sort')
        order = kwargs.get('order')
        return active_submission_table(submission, file, error_level, page, limit, sort, order)
