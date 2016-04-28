import os, os.path
import sys
import traceback
from flask.ext.cors import CORS
from flask.ext.bcrypt import Bcrypt
from flask import Flask ,send_from_directory
from dataactcore.utils.cloudLogger import CloudLogger
from dataactcore.utils.jsonResponse import JsonResponse
from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactbroker.handlers.accountHandler import AccountHandler
from dataactbroker.handlers.aws.session import DynamoInterface, SessionTable
from dataactbroker.fileRoutes import add_file_routes
from dataactbroker.loginRoutes import add_login_routes
from dataactbroker.userRoutes import add_user_routes
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES, CONFIG_DB, CONFIG_PATH


def createApp():
    """Set up the application."""
    try :
        # Create application
        app = Flask(__name__, instance_path=CONFIG_PATH)
        local = CONFIG_BROKER['local']
        app.config.from_object(__name__)
        app.config['LOCAL'] = local
        app.config['REST_TRACE'] = CONFIG_SERVICES['rest_trace']
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

        # When runlocal is true, assume Dynamo is on the same server
        # (should be false for prod)
        JsonResponse.debugMode = app.config['REST_TRACE']

        if CONFIG_SERVICES['cross_origin_url'] ==  "*":
            cors = CORS(app, supports_credentials=True)
        else:
            cors = CORS(app, supports_credentials=True,
                origins=CONFIG_SERVICES['cross_origin_url'])
        # Enable AWS Sessions
        app.session_interface = DynamoInterface()
        # Set up bcrypt
        bcrypt = Bcrypt(app)
        # Root will point to index.html
        @app.route("/", methods=["GET"])
        def root():
            return "Broker is running"

        if local:
            localFiles = os.path.join(broker_file_path, "<path:filename>")
            # Only define this route when running locally
            @app.route(localFiles)
            def sendFile(filename):
                if(config["local"]) :
                    return send_from_directory(broker_file_path, filename)
        else:
            # For non-local installs, set Dynamo Region
            SessionTable.DYNAMO_REGION = CONFIG_BROKER['aws_region']

        # Add routes for modules here
        add_login_routes(app, bcrypt)

        add_file_routes(app, CONFIG_BROKER['aws_create_temp_credentials'],
            local, broker_file_path)
        add_user_routes(app, app.config['SYSTEM_EMAIL'], bcrypt)

        SessionTable.LOCAL_PORT = CONFIG_DB['dynamo_port']

        SessionTable.setup(app, local)

        return app

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        trace = traceback.extract_tb(exc_tb, 10)
        CloudLogger.logError('Broker App Level Error: ', e, trace)

        del exc_tb
        raise


def runApp():
    """runs the application"""
    app = createApp()
    debugFlag = CONFIG_SERVICES['server_debug']
    app.run(
        debug=debugFlag,
        threaded=True,
        host=CONFIG_SERVICES['broker_api_host'],
        port=CONFIG_SERVICES['broker_api_port']
    )

if __name__ == '__main__':
    runApp()
elif __name__[0:5]=="uwsgi":
    app = createApp()