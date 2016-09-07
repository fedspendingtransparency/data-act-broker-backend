from dataactbroker.scripts.setupEmails import setupEmails
from dataactcore.scripts.setupAllDB import setupAllDB
from dataactcore.utils.responseException import ResponseException
from dataactbroker.handlers.userHandler import UserHandler
from dataactbroker.handlers.aws.session import SessionTable
from dataactcore.config import CONFIG_BROKER, CONFIG_DB
import argparse
from flask_bcrypt import Bcrypt
from sqlalchemy.orm.exc import NoResultFound


def options():
    """ Run functions based on arguments provided """
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--initialize", action="store_true", help="Runs all of the setup options")
    parser.add_argument("-a", "--createAdmin", action="store_true", help="Creates admin user")
    parser.add_argument("-s", "--start", action="store_true", help="Starts the broker")
    parser.add_argument("-d", "--deploy", action="store_true", help="Deploy on AWS")
    args = parser.parse_args()
    optionsDict = vars(args)

    noArgs = True
    for arg in optionsDict:
        if(optionsDict[arg]):
            noArgs = False
            globals()[arg]()

    if noArgs:
        print ("Please enter an argument.")

def deploy():
    """ Run steps needed for deployment on AWS, currently this is just DB setup """
    setupDB()

def initialize():
    """ Set up databases and dynamo and create an admin user """
    print ("Setting up databases...")
    setupDB()
    print ("Setting up DynamoDB session table...")
    setupSessionTable()
    print ("Creating admin user...")
    createAdmin()
    print ("The broker has been initialized. You may now run the broker with the --start argument.")


def setupDB():
    """ Setup all databases used by API """
    setupAllDB()
    setupEmails()


def createAdmin():
    """Create initial admin user."""
    adminEmail = CONFIG_BROKER['admin_email']
    adminPass = CONFIG_BROKER['admin_password']
    userDb = UserHandler()
    try:
        user = userDb.getUserByEmail(adminEmail)
    except ResponseException as e:

        if type(e.wrappedException) is NoResultFound:
            userDb.createUserWithPassword(
                adminEmail, adminPass, Bcrypt(), permission=2)
            user = userDb.getUserByEmail(adminEmail)
            userDb.addUserInfo(user, "Admin", "SYS", "System Admin")
    userDb.close()


def setupSessionTable():
    """ Create Dynamo session table """
    SessionTable.createTable(CONFIG_BROKER['local'], CONFIG_DB['dynamo_port'])


def start():
    """ Launches the app """
    from dataactbroker.app import runApp
    runApp()

if __name__ == '__main__':
    options()
