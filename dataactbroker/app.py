import os
import inspect
import flask
from flask.ext.cors import CORS
from flask.ext.bcrypt import Bcrypt
from flask import Flask
import json
from dataactcore.utils.jsonResponse import JsonResponse
from dataactbroker.handlers.aws.sesEmail import sesEmail
from dataactbroker.handlers.accountHandler import AccountHandler
from dataactbroker.handlers.aws.session import DynamoInterface, SessionTable
from dataactbroker.fileRoutes import add_file_routes
from dataactbroker.loginRoutes import add_login_routes
from dataactbroker.userRoutes import add_user_routes

def runApp():
    """Set up the Application"""
    # Create application
    config_path = "".join([os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),"/config"])
    app = Flask(__name__,instance_path=config_path)

    def getAppConfiguration() :
        """gets the web_api_configuration JSON"""
        configFile = "".join([app.instance_path, "/web_api_configuration.json"])
        return json.loads(open(configFile,"r").read())

    config = getAppConfiguration()
    # Set parameters
    AccountHandler.FRONT_END = config["frontend_url"]
    sesEmail.SIGNING_KEY =  config["security_key"]
    debugFlag = config["server_debug"]  # Should be false for prod
    runLocal = config["local_dynamo"]  # False for prod, when True this assumes that the Dynamo is on the same server
    JsonResponse.debugMode = config["rest_trace"]

    app.config.from_object(__name__)
    if(config["origins"] ==  "*"):
        cors = CORS(app,supports_credentials=True)
    else:
        cors = CORS(app,supports_credentials=True,origins=config["origins"])
    #Enable AWS Sessions
    app.session_interface = DynamoInterface()
    # Set up bcrypt
    bcrypt = Bcrypt(app)
    # Root will point to index.html
    @app.route("/", methods=["GET"])
    def root():
        return "Broker is running"


    # Add routes for modules here
    add_login_routes(app,bcrypt)
    add_file_routes(app,config["create_credentials"])
    add_user_routes(app,config["system_email"],bcrypt)
    SessionTable.localPort  = int( config["dynamo_port"])
    SessionTable.setup(app, runLocal)
    app.run(debug=debugFlag,threaded=True,host="0.0.0.0",port= int(config["port"]))

if __name__ == '__main__':
    runApp()
