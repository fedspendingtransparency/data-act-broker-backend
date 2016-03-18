import os
import sys
import inspect
import traceback
import json
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


def getAppConfiguration(app):
    """gets the web_api_configuration JSON"""
    configFile = "".join([app.instance_path, "/web_api_configuration.json"])
    return json.loads(open(configFile, "r").read())


def createApp():
    """Set up the Application"""
    try :
        # Create application
        config_path = "".join([os.path.dirname(os.path.abspath(
            inspect.getfile(inspect.currentframe()))), "/config"])
        app = Flask(__name__, instance_path=config_path)
        config = getAppConfiguration(app)
        if(config["local"]) :
            # Up to 1 GB can be uploaded for local
            app.config['MAX_CONTENT_LENGTH'] = 1024 * 1024 * 1024

        # Set parameters
        AccountHandler.FRONT_END = config["frontend_url"]
        sesEmail.SIGNING_KEY =  config["security_key"]
        sesEmail.isLocal = config["local"]
        sesEmail.emailLog = "".join([config["local_folder"], "/email.log"])
        # If local, make the email directory if needed
        if(config["local"] and not os.path.exists(config["local_folder"])):
            os.makedirs(config["local_folder"])
        # When runlocal is true, assume Dynamo is on the same server
        # (should be false for prod)
        runLocal = config["local_dynamo"]
        JsonResponse.debugMode = config["rest_trace"]

        app.config.from_object(__name__)

        if(config["origins"] ==  "*"):
            cors = CORS(app, supports_credentials=True)
        else:
            cors = CORS(app, supports_credentials=True,
                origins=config["origins"])
        # Enable AWS Sessions
        app.session_interface = DynamoInterface()
        # Set up bcrypt
        bcrypt = Bcrypt(app)
        # Root will point to index.html
        @app.route("/", methods=["GET"])
        def root():
            return "Broker is running"

        localFiles = "".join([config["local_folder"], "/<path:filename>"])

        @app.route(localFiles)
        def sendFile(filename):
            if(config["local"]) :
                return send_from_directory(config["local_folder"], filename)

        # Add routes for modules here
        add_login_routes(app, bcrypt)
        add_file_routes(app, config["create_credentials"],
            config["local"], config["local_folder"])
        add_user_routes(app, config["system_email"], bcrypt)
        SessionTable.localPort = int(config["dynamo_port"])
        SessionTable.setup(app, runLocal)

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
    config = getAppConfiguration(app)
    debugFlag = config["server_debug"]
    app.run(
        debug=debugFlag,
        threaded=True,
        host="0.0.0.0",
        port=int(config["port"]))

if __name__ == '__main__':
    runApp()
