import os
import os.path

from flask_cors import CORS
from flask_bcrypt import Bcrypt
from flask import Flask, g, session

from dataactbroker.exception_handler import add_exception_handlers
from dataactbroker.handlers.account_handler import AccountHandler
from dataactbroker.handlers.aws.sesEmail import SesEmail
from dataactbroker.handlers.aws.session import UserSessionInterface

from dataactbroker.routes.domain_routes import add_domain_routes
from dataactbroker.routes.file_routes import add_file_routes
from dataactbroker.routes.generation_routes import add_generation_routes
from dataactbroker.routes.login_routes import add_login_routes
from dataactbroker.routes.user_routes import add_user_routes

from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.userModel import User

from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

# DataDog Import (the below value gets changed via Ansible during deployment. DO NOT DELETE)
USE_DATADOG = False

if USE_DATADOG:
    from ddtrace import tracer
    from ddtrace.contrib.flask import TraceMiddleware


def create_app():
    """Set up the application."""
    flask_app = Flask(__name__.split('.')[0])
    local = CONFIG_BROKER['local']
    flask_app.config.from_object(__name__)
    flask_app.config['LOCAL'] = local
    flask_app.debug = CONFIG_SERVICES['debug']
    flask_app.env = 'development' if CONFIG_SERVICES['debug'] else 'production'
    flask_app.config['SYSTEM_EMAIL'] = CONFIG_BROKER['reply_to_email']

    # Future: Override config w/ environment variable, if set
    flask_app.config.from_envvar('BROKER_SETTINGS', silent=True)

    # Set parameters
    broker_file_path = CONFIG_BROKER['broker_files']
    AccountHandler.FRONT_END = CONFIG_BROKER['full_url']
    SesEmail.SIGNING_KEY = CONFIG_BROKER['email_token_key']
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

    # Root will point to index.html
    @flask_app.route("/", methods=["GET"])
    def root():
        return "Broker is running"

    @flask_app.errorhandler(ResponseException)
    def handle_response_exception(exception):
        return JsonResponse.error(exception, exception.status)

    @flask_app.errorhandler(Exception)
    def handle_exception(exception):
        wrapped = ResponseException(str(exception), StatusCode.INTERNAL_ERROR, type(exception))
        return JsonResponse.error(wrapped, wrapped.status)

    # Add routes for modules here
    add_login_routes(flask_app, bcrypt)

    add_file_routes(flask_app, local, broker_file_path)
    add_generation_routes(flask_app, local, broker_file_path)
    add_user_routes(flask_app, flask_app.config['SYSTEM_EMAIL'], bcrypt)
    add_domain_routes(flask_app)
    add_exception_handlers(flask_app)
    return flask_app


def run_app():
    """runs the application"""
    flask_app = create_app()

    # This is for DataDog (Do Not Delete)
    if USE_DATADOG:
        TraceMiddleware(flask_app, tracer, service="broker-dd", distributed_tracing=False)

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
