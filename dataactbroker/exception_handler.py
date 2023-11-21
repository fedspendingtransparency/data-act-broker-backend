import traceback

from flask import jsonify

from dataactcore.config import CONFIG_SERVICES


def add_exception_handlers(app):
    @app.errorhandler(422)
    def handle_invalid_usage(error):
        """We receive 422s from the webargs library. Clean up their message
        and convert them to 400s"""
        if hasattr(error, 'data'):
            webargs_messages = []
            for location, fielddata in error.data['messages'].items():
                for field_name, messages in sorted(fielddata.items()):
                    field_messages = []
                    for message in messages:
                        if "Must be one of:" in message:
                            options = ', '.join(sorted([x.strip() for x in message[15:-1].split(', ')]))
                            message = f"Must be one of: {options}."
                        field_messages.append(message)
                    webargs_messages.append(field_name + ': ' + '; '.join(field_messages))
            resp_message = ' '.join(webargs_messages)
        else:
            resp_message = 'Invalid request'
        body = {'message': resp_message}
        if CONFIG_SERVICES['debug']:
            body['exception_type'] = str(error.exc)
            body['trace'] = [
                str(entry)
                for entry in traceback.extract_tb(error.exc.__traceback__, 10)
            ]
        return jsonify({'message': resp_message}), 400
