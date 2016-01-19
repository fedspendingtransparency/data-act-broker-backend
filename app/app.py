
import os
import inspect
from flask.ext.cors import CORS
from flask import Flask
import json

from handlers.aws.session import DynamoInterface, SessionTable, LoginSession
from handlers.managerProxy import ManagerProxy
from fileRoutes import add_file_routes
from loginRoutes import add_login_routes
from dataactcore.utils.jsonResponse import JsonResponse


def getAppConfiguration() :
    path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    lastBackSlash = path.rfind("\\",0,-1)
    lastForwardSlash = path.rfind("/",0,-1)
    lastSlash = max([lastBackSlash,lastForwardSlash])
    configFile = path[0:lastSlash] + "/web_api_configuration.json"
    return json.loads(open(configFile,"r").read())

config = getAppConfiguration()
# Set parameters
debugFlag = config["ServerDebug"]  # Should be false for prod
runLocal = config["LocalDynamo"]  # False for prod, when True this assumes that the Dynamo is on the same server
createTable = config["CreateDynamo"]  # Should be false for most runs, true for first run with DynamoDB
JsonResponse.debugMode = config["JSONDebug"]
# Get the project's root folder
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

# Create application
app = Flask(__name__)
app.config.from_object(__name__)
if(config["Origins"] ==  "*") :
    cors = CORS(app,supports_credentials=True)
else :
    cors = CORS(app,supports_credentials=True,origins=config["Origins"])
#Enable AWS Sessions
app.session_interface = DynamoInterface()


def getAppConfig() :
    path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    lastBackSlash = path.rfind("\\",0,-1)
    lastForwardSlash = path.rfind("/",0,-1)
    lastSlash = max([lastBackSlash,lastForwardSlash])
    configFile = path[0:lastSlash] + "/" + ManagerProxy.MANAGER_FILE
    return json.loads(open(configFile,"r").read())



# Root will point to index.html
@app.route("/", methods=["GET"])
def root():
    return "Broker is running"


# Add routes for modules here
add_login_routes(app)
add_file_routes(app)

if __name__ == '__main__':
    SessionTable.setup(app, runLocal, createTable)
    app.run(debug=debugFlag,threaded=True,host="0.0.0.0",port=80)
