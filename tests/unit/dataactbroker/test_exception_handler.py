import json

from webargs import fields as webargs_fields
from webargs.flaskparser import use_kwargs

from dataactbroker import exception_handler


def test_exception_handler(test_app):
    exception_handler.add_exception_handlers(test_app.application)

    @test_app.application.route("/endpoint/")
    @use_kwargs({
        'param1': webargs_fields.Int(),
        'param2': webargs_fields.String(required=True),
        'param3': webargs_fields.Int(required=True)
    }, location='query')
    def handle(param1, param2):
        pass

    result = test_app.get('/endpoint/?param1=not-a-string&param3=3')
    assert result.status_code == 400
    result = json.loads(result.data.decode('UTF-8'))
    assert result['message'] == ('param1: Not a valid integer. '
                                 'param2: Missing data for required field.')
