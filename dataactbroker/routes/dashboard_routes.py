from webargs import fields as webargs_fields
from webargs.flaskparser import use_kwargs

from dataactbroker.permissions import requires_login

from dataactbroker.handlers.dashboard_handler import historic_dabs_warning_summary, list_rule_labels


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
        files = kwargs.get('files')
        fabs = kwargs.get('fabs')
        error_level = kwargs.get('error_level')
        return list_rule_labels(files, fabs, error_level)

    @app.route("/v1/historic_dabs_summary/", methods=["POST"])
    @requires_login
    @use_kwargs({
        'filters': webargs_fields.Dict(keys=webargs_fields.String(), missing={})
    })
    def historic_dabs_summary(**kwargs):
        filters = kwargs.get('filters')
        return historic_dabs_warning_summary(filters)
