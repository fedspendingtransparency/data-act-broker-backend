from webargs import fields as webargs_fields, validate as webargs_validate
from webargs.flaskparser import use_kwargs

from dataactbroker.permissions import requires_agency_code_perms

from dataactbroker.handlers.settings_handler import list_rule_settings, save_rule_settings
from dataactbroker.handlers.dashboard_handler import FILE_TYPES


# Add the settings routes
def add_settings_routes(app):
    """ Create routes related to settings """

    @app.route('/v1/rule_settings/', methods=['GET'])
    @requires_agency_code_perms('submitter')
    @use_kwargs({
        'agency_code': webargs_fields.String(required=True),
        'file': webargs_fields.String(validate=webargs_validate.
                                      OneOf(FILE_TYPES, error='Must be {}, or {}'.format(', '.join(FILE_TYPES[:-1]),
                                                                                         FILE_TYPES[-1])),
                                      required=True)
    })
    def get_rule_settings(**kwargs):
        """ Returns the rule settings based on the filters provided """
        agency_code = kwargs.get('agency_code')
        file = kwargs.get('file')
        return list_rule_settings(agency_code, file)

    @app.route('/v1/save_rule_settings/', methods=['POST'])
    @requires_agency_code_perms('submitter')
    @use_kwargs({
        'agency_code': webargs_fields.String(required=True),
        'file': webargs_fields.String(validate=webargs_validate.
                                      OneOf(FILE_TYPES, error='Must be {}, or {}'.format(', '.join(FILE_TYPES[:-1]),
                                                                                         FILE_TYPES[-1])),
                                      required=True),
        'errors': webargs_fields.List(webargs_fields.Dict),
        'warnings': webargs_fields.List(webargs_fields.Dict)
    })
    def post_save_rule_settings(**kwargs):
        """ Set the rule settings based on the rules provided """
        agency_code = kwargs.get('agency_code')
        file = kwargs.get('file')
        errors = kwargs.get('errors', [])
        warnings = kwargs.get('warnings', [])
        return save_rule_settings(agency_code, file, errors, warnings)
