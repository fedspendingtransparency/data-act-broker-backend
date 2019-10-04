from webargs import fields as webargs_fields
from webargs.flaskparser import use_kwargs

from dataactbroker.permissions import requires_login

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
