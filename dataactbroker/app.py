
import os
import inspect
from flask.ext.cors import CORS
from flask import Flask
import json

from dataactbroker.handlers.aws.session import DynamoInterface, SessionTable, LoginSession
from dataactbroker.handlers.managerProxy import ManagerProxy
from dataactbroker.fileRoutes import add_file_routes
from dataactbroker.loginRoutes import add_login_routes
from dataactcore.utils.jsonResponse import JsonResponse

def runApp():
    """Set up the Application"""
    def getAppConfiguration() :
        """gets the web_api_configuration JSON"""
        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        lastBackSlash = path.rfind("\\",0,-1)
        lastForwardSlash = path.rfind("/",0,-1)
        lastSlash = max([lastBackSlash,lastForwardSlash])
        configFile = path[0:lastSlash] + "/web_api_configuration.json"
        return json.loads(open(configFile,"r").read())

    config = getAppConfiguration()
    # Set parameters
    debugFlag = config["server_debug"]  # Should be false for prod
    runLocal = config["local_dynamo"]  # False for prod, when True this assumes that the Dynamo is on the same server
    JsonResponse.debugMode = config["rest_trace"]
     #Allows for real Credentials to be created for S3 uploads
    # Get the project's root folder
    PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

    # Create application
    app = Flask(__name__)
    app.config.from_object(__name__)
    if(config["Origins"] ==  "*"):
        cors = CORS(app,supports_credentials=True)
    else:
        cors = CORS(app,supports_credentials=True,origins=config["origins"])
    #Enable AWS Sessions
    app.session_interface = DynamoInterface()


    # Root will point to index.html
    @app.route("/", methods=["GET"])
    def root():
        return "Broker is running"


    # Add routes for modules here
    add_login_routes(app)
    add_file_routes(app,config["CreateCredentials"])

    SessionTable.setup(app, runLocal)
    app.run(debug=debugFlag,threaded=True,host="0.0.0.0",port= config["port"])

if __name__ == '__main__':
    runApp()
