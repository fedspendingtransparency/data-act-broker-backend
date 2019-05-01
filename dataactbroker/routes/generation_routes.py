from webargs import fields as webargs_fields, validate as webargs_validate
from webargs.flaskparser import use_kwargs

from dataactbroker.decorators import convert_to_submission_id
from dataactbroker.handlers import generation_handler
from dataactbroker.permissions import requires_login, requires_submission_perms

DATE_REGEX = '^\d{2}\/\d{2}\/\d{4}$'


def add_generation_routes(app, is_local, server_path):
    """ Create routes related to file generation

        Attributes:
            app: A Flask application
            is_local: A boolean flag indicating whether the application is being run locally or not
            server_path: A string containing the path to the server files (only applicable when run locally)
    """

    @app.route("/v1/generate_file/", methods=["POST"])
    @convert_to_submission_id
    @requires_submission_perms('writer')
    @use_kwargs({
        'file_type': webargs_fields.String(
            required=True,
            validate=webargs_validate.OneOf(('D1', 'D2', 'E', 'F'), error="Must be either D1, D2, E or F")),
        'start': webargs_fields.String(
            validate=webargs_validate.Regexp(DATE_REGEX, error="Must be in the format MM/DD/YYYY")),
        'end': webargs_fields.String(
            validate=webargs_validate.Regexp(DATE_REGEX, error="Must be in the format MM/DD/YYYY")),
        'agency_type': webargs_fields.String(
            missing='awarding',
            validate=webargs_validate.OneOf(('awarding', 'funding'),
                                            error="Must be either awarding or funding if provided")
        )
    })
    def generate_file(submission_id, file_type, **kwargs):
        """ Kick of a file generation, or retrieve the cached version of the file.

            Attributes:
                submission: submission ID for which we're generating the file
                file_type: type of file to generate the job for
                start: the start date for the file to generate
                end: the end date for the file to generate
                agency_type: The type of agency (awarding or funding) to generate the file for
        """
        start = kwargs.get('start')
        end = kwargs.get('end')
        agency_type = kwargs.get('agency_type')
        return generation_handler.generate_file(submission_id, file_type, start, end, agency_type)

    @app.route("/v1/check_generation_status/", methods=["GET"])
    @convert_to_submission_id
    @requires_submission_perms('reader')
    @use_kwargs({'file_type': webargs_fields.String(
        required=True,
        validate=webargs_validate.OneOf(('D1', 'D2', 'E', 'F'), error="Must be either D1, D2, E or F"))
    })
    def check_generation_status(submission, file_type):
        """ Return status of file generation job

            Attributes:
                submission: submission for which we're generating the file
                file_type: type of file to check the status of
        """
        return generation_handler.check_generation(submission, file_type)

    @app.route("/v1/generate_detached_file/", methods=["POST"])
    @requires_login
    @use_kwargs({
        'file_type': webargs_fields.String(required=True, validate=webargs_validate.OneOf(('A', 'D1', 'D2'))),
        'cgac_code': webargs_fields.String(),
        'frec_code': webargs_fields.String(),
        'start': webargs_fields.String(),
        'end': webargs_fields.String(),
        'year': webargs_fields.Int(),
        'period': webargs_fields.Int(validate=webargs_validate.OneOf(list(range(2, 13)),
                                                                     error="Period must be an integer 2-12.")),
        'agency_type': webargs_fields.String(
            missing='awarding',
            validate=webargs_validate.OneOf(('awarding', 'funding'),
                                            error="Must be either awarding or funding if provided")
        )
    })
    def generate_detached_file(file_type, **kwargs):
        """ Generate a file from external API, independent from a submission

            Attributes:
                file_type: type of file to be generated
                cgac_code: the code of a CGAC agency if generating for a CGAC agency
                frec_code: the code of a FREC agency if generating for a FREC agency
                start: start date in a string, formatted MM/DD/YYYY
                end: end date in a string, formatted MM/DD/YYYY
                year: integer indicating the year to generate for (YYYY)
                period: integer indicating the period to generate for (2-12)
                agency_type: The type of agency (awarding or funding) to generate the file for
        """
        cgac_code = kwargs.get('cgac_code')
        frec_code = kwargs.get('frec_code')
        start = kwargs.get('start')
        end = kwargs.get('end')
        year = kwargs.get('year')
        period = kwargs.get('period')
        agency_type = kwargs.get('agency_type')
        return generation_handler.generate_detached_file(file_type, cgac_code, frec_code, start, end, year, period,
                                                         agency_type)

    @app.route("/v1/check_detached_generation_status/", methods=["GET"])
    @requires_login
    @use_kwargs({'job_id': webargs_fields.Int(required=True)})
    def check_detached_generation_status(job_id):
        """ Return status of file generation job """
        return generation_handler.check_detached_generation(job_id)
