from dataactbroker.exceptions.invalid_usage import InvalidUsage
from flask import jsonify


def add_exception_handlers(app):

    @app.errorhandler(InvalidUsage)
    def handle_invalid_usage(error):
        print("handling invalid usage error...")
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response