import traceback

from flask import jsonify

from dataactcore.config import CONFIG_SERVICES


def add_exception_handlers(app):
    @app.errorhandler(422)
    def handle_invalid_usage(error):
        if hasattr(error, 'data'):
            message = ' '.join(
                field_name + ': ' + '; '.join(messages)
                for field_name, messages in error.data['messages'].items()
            )
        else:
            message = 'Invalid request'
        body = {'message': message}
        if CONFIG_SERVICES['debug']:
            body['exception_type'] = str(error.exc)
            body['trace'] = [
                str(entry)
                for entry in traceback.extract_tb(error.exc.__traceback__, 10)
            ]
        return jsonify({'message': message}), 400
