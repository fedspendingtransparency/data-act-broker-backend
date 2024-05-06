import ddtrace
import json
import logging
import os
import os.path

from flask_cors import CORS
from flask_bcrypt import Bcrypt
from flask import Flask, g, session, request

from dataactbroker.exception_handler import add_exception_handlers
from dataactbroker.handlers.account_handler import AccountHandler
from dataactbroker.handlers.aws.sesEmail import SesEmail
from dataactbroker.handlers.aws.session import UserSessionInterface

from dataactbroker.routes.domain_routes import add_domain_routes
from dataactbroker.routes.file_routes import add_file_routes
from dataactbroker.routes.generation_routes import add_generation_routes
from dataactbroker.routes.login_routes import add_login_routes
from dataactbroker.routes.user_routes import add_user_routes
from dataactbroker.routes.dashboard_routes import add_dashboard_routes
from dataactbroker.routes.settings_routes import add_settings_routes

from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.broker_logging import configure_logging
from dataactcore.models.userModel import User

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.ResponseError import ResponseError
from dataactcore.utils.statusCode import StatusCode

logger = logging.getLogger(__name__)

# Replace below param with enabled=True during env-deploys to turn on
ddtrace.tracer.configure(enabled=False)
if ddtrace.tracer.enabled:
    ddtrace.config.flask["service_name"] = "api"
    ddtrace.config.flask["analytics_enabled"] = True  # capture APM "Traces" & "Analyzed Spans" in App Analytics
    ddtrace.config.flask["analytics_sample_rate"] = 1.0  # Including 100% of traces in sample
    ddtrace.config.flask["trace_query_string"] = True
    # Distributed tracing only needed if picking up disjoint traces by HTTP Header value
    ddtrace.config.flask["distributed_tracing_enabled"] = False
    # Trace HTTP Request or Response Headers listed in this whitelist
    ddtrace.config.trace_headers(
        [
            "content-length",  # req and resp
            "content-type",  # req and resp
            "host",
            "origin",
            "referer",
            "user-agent",
            "x-forwarded-for",
            "x-requested-with",
            "x-session-id",
            # Response Headers
            "allow",
            "strict-transport-security",
        ]
    )
    # patch_all() captures traces from integrated components' libraries by patching them. See:
    # - http://pypi.datadoghq.com/trace/docs/advanced_usage.html#patch-all
    # - Integrated Libs: http://pypi.datadoghq.com/trace/docs/index.html#supported-libraries
    ddtrace.patch_all()


def create_app():
    """Set up the application."""
    flask_app = Flask(__name__.split('.')[0])
    local = CONFIG_BROKER['local']
    flask_app.config.from_object(__name__)
    flask_app.config['LOCAL'] = local
    flask_app.debug = CONFIG_SERVICES['debug']
    flask_app.env = 'development' if CONFIG_SERVICES['debug'] else 'production'
    flask_app.config['SYSTEM_EMAIL'] = CONFIG_BROKER['reply_to_email']
    # Make the app not care if there's a trailing slash or not
    flask_app.url_map.strict_slashes = False

    # Future: Override config w/ environment variable, if set
    flask_app.config.from_envvar('BROKER_SETTINGS', silent=True)

    # Set parameters
    broker_file_path = CONFIG_BROKER['broker_files']
    AccountHandler.FRONT_END = CONFIG_BROKER['full_url']
    SesEmail.is_local = local
    if SesEmail.is_local:
        SesEmail.emailLog = os.path.join(broker_file_path, 'email.log')
    # If local, make the email directory if needed
    if local and not os.path.exists(broker_file_path):
        os.makedirs(broker_file_path)

    JsonResponse.debugMode = flask_app.debug

    if CONFIG_SERVICES['cross_origin_url'] == "*":
        CORS(flask_app, supports_credentials=False, allow_headers="*", expose_headers="X-Session-Id")
    else:
        CORS(flask_app, supports_credentials=False, origins=CONFIG_SERVICES['cross_origin_url'],
             allow_headers="*", expose_headers="X-Session-Id")
    # Enable DB session table handling
    flask_app.session_interface = UserSessionInterface()
    # Set up bcrypt
    bcrypt = Bcrypt(flask_app)

    @flask_app.teardown_appcontext
    def teardown_appcontext(exception):
        GlobalDB.close()

    @flask_app.before_request
    def before_request():
        # Set global value for local
        g.is_local = local
        sess = GlobalDB.db().session
        # setup user
        g.user = None
        if session.get('name') is not None:
            g.user = sess.query(User).filter_by(user_id=session['name']).one_or_none()

        # Verbose Logs for Incoming Requests
        # request_dict = {
        #     'url': request.url,
        #     'headers': request.headers,
        #     'request': request.get_data()
        # }
        # logger.info(request_dict)

        content_type = request.headers.get('Content-Type')

        # If the request is a POST we want to log the request body
        if request.method == 'POST' and content_type and 'login' not in request.url.lower():
            request_body = {}

            # If request is json, turn it into a dict
            if 'application/json' in content_type:
                request_body = json.loads(request.get_data().decode('utf8'))
            elif 'multipart/form-data' in content_type:
                # If request is a multipart request, get only the form portions of it
                for key in request.form.keys():
                    request_body[key] = request.form[key]

            request_dict = {
                'message': 'Request body for ' + request.url,
                'body': request_body
            }
            logger.info(request_dict)

    # Root will point to index.html
    @flask_app.route("/", methods=["GET"])
    def root():
        return "Broker is running"

    @flask_app.errorhandler(ResponseError)
    def handle_response_exception(exception):
        return JsonResponse.error(exception, exception.status)

    @flask_app.errorhandler(Exception)
    def handle_exception(exception):
        wrapped = ResponseError(str(exception), StatusCode.INTERNAL_ERROR, type(exception))
        return JsonResponse.error(wrapped, wrapped.status)

    # Add routes for modules here
    add_login_routes(flask_app, bcrypt)

    add_file_routes(flask_app, local, broker_file_path)
    add_generation_routes(flask_app, local, broker_file_path)
    add_user_routes(flask_app, flask_app.config['SYSTEM_EMAIL'], bcrypt)
    add_dashboard_routes(flask_app)
    add_settings_routes(flask_app)
    add_domain_routes(flask_app)
    add_exception_handlers(flask_app)
    return flask_app


def run_app():
    """runs the application"""
    flask_app = create_app()
    flask_app.run(
        threaded=True,
        host=CONFIG_SERVICES['broker_api_host'],
        port=CONFIG_SERVICES['broker_api_port']
    )


if __name__ == '__main__':
    configure_logging()
    run_app()

elif __name__[0:5] == "uwsgi":
    configure_logging()
    app = create_app()
