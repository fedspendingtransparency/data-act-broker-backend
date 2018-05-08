from functools import wraps
from webargs import fields as webargs_fields
from webargs.flaskparser import parser as webargs_parser

from dataactbroker.permissions import requires_login

from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode


def convert_to_submission_id(fn):
    """Decorator which reads the request, looking for a submission key to
    convert into a submission_id parameter. The provided function should have
    a submission_id parameter as its first argument."""
    @wraps(fn)
    @requires_login     # check login before checking submission_id
    def wrapped(*args, **kwargs):
        req_args = webargs_parser.parse({
            'submission': webargs_fields.Int(),
            'submission_id': webargs_fields.Int()
        })
        submission_id = req_args.get('submission', req_args.get('submission_id'))
        if submission_id is None:
            raise ResponseException(
                "submission_id is required", StatusCode.CLIENT_ERROR)
        return fn(submission_id, *args, **kwargs)
    return wrapped
