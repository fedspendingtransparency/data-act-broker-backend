from webargs import fields as webargs_fields, validate as webargs_validate
from webargs.flaskparser import use_kwargs

from dataactbroker.permissions import requires_login

from dataactbroker.handlers.dashboard_handler import (historic_dabs_warning_summary, historic_dabs_warning_table,
                                                      list_rule_labels)


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
