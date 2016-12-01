import os, os.path

from flask_cors import CORS
from flask_bcrypt import Bcrypt
from flask import Flask

from dataactbroker.domainRoutes import add_domain_routes
from dataactbroker.exception_handler import add_exception_handlers
from dataactbroker.fileRoutes import add_file_routes
from dataactbroker.handlers.accountHandler import AccountHandler
from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactbroker.handlers.aws.session import UserSessionInterface
from dataactbroker.loginRoutes import add_login_routes
from dataactbroker.userRoutes import add_user_routes
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode


def createApp():
    """Set up the application."""
    app = Flask(__name__.split('.')[0])
    local = CONFIG_BROKER['local']
    app.config.from_object(__name__)
    app.config['LOCAL'] = local
    app.debug = CONFIG_SERVICES['debug']
    app.config['SYSTEM_EMAIL'] = CONFIG_BROKER['reply_to_email']

    # Future: Override config w/ environment variable, if set
    app.config.from_envvar('BROKER_SETTINGS', silent=True)

    # Set parameters
    broker_file_path = CONFIG_BROKER['broker_files']
    AccountHandler.FRONT_END = CONFIG_BROKER['full_url']
    sesEmail.SIGNING_KEY =  CONFIG_BROKER['email_token_key']
    sesEmail.isLocal = local
    if sesEmail.isLocal:
        sesEmail.emailLog = os.path.join(broker_file_path, 'email.log')
    # If local, make the email directory if needed
    if local and not os.path.exists(broker_file_path):
        os.makedirs(broker_file_path)

    JsonResponse.debugMode = app.debug

    if CONFIG_SERVICES['cross_origin_url'] ==  "*":
        cors = CORS(app, supports_credentials=False, allow_headers = "*", expose_headers = "X-Session-Id")
    else:
        cors = CORS(app, supports_credentials=False, origins=CONFIG_SERVICES['cross_origin_url'],
                    allow_headers = "*", expose_headers = "X-Session-Id")
    # Enable DB session table handling
    app.session_interface = UserSessionInterface()
    # Set up bcrypt
    bcrypt = Bcrypt(app)

    @app.teardown_appcontext
    def teardown_appcontext(exception):
        GlobalDB.close()

    @app.before_request
    def before_request():
        GlobalDB.db()

    # Root will point to index.html
    @app.route("/", methods=["GET"])
    def root():
        return "Broker is running"

    @app.errorhandler(ResponseException)
    def handle_response_exception(exception):
        return JsonResponse.error(exception, exception.status)

    @app.errorhandler(Exception)
    def handle_exception(exception):
        wrapped = ResponseException(str(exception), StatusCode.INTERNAL_ERROR,
                                    type(exception))
        return JsonResponse.error(wrapped, wrapped.status)

    # Add routes for modules here
    add_login_routes(app, bcrypt)

    add_file_routes(app, CONFIG_BROKER['aws_create_temp_credentials'],
        local, broker_file_path, bcrypt)
    add_user_routes(app, app.config['SYSTEM_EMAIL'], bcrypt)
    add_domain_routes(app, local, bcrypt)
    add_exception_handlers(app)
    return app

def runApp():
    """runs the application"""
    app = createApp()
    app.run(
        threaded=True,
        host=CONFIG_SERVICES['broker_api_host'],
        port=CONFIG_SERVICES['broker_api_port']
    )

if __name__ == '__main__':
    configure_logging()
    runApp()

elif __name__[0:5]=="uwsgi":
    configure_logging()
    app = createApp()
