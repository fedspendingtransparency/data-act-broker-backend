from dataactbroker.scripts.setupEmails import setupEmails
from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupUserDB import setupUserDB
from dataactbroker.handlers.userHandler import UserHandler
from dataactbroker.handlers.aws.session import SessionTable
from dataactcore.config import CONFIG_BROKER, CONFIG_DB
import os
import argparse
from flask.ext.bcrypt import Bcrypt


def options():
    if os.getuid() != 0:
        print ("Please run this script with sudo")

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--initialize", action="store_true", help="Runs all of the setup options")
    parser.add_argument("-db", "--setupDB", action="store_true", help="Creates the database schema")
    parser.add_argument("-a", "--createAdmin", action="store_true", help="Creates admin user")
    parser.add_argument("-s", "--start", action="store_true", help="Starts the broker")
    args = parser.parse_args()
    optionsDict = vars(args)

    noArgs = True
    for arg in optionsDict:
        if(optionsDict[arg]):
            noArgs = False
            globals()[arg]()

    if noArgs:
        print ("Please enter an argument.")


def initialize():
    print ("Setting up databases...")
    setupDB()
    print ("Setting up DynamoDB session table...")
    setupSessionTable()
    print ("Creating admin user...")
    createAdmin()
    print ("The broker has been initialized. You may now run the broker with the --start argument.")


def setupDB():
    setupJobTrackerDB(hardReset=True)
    setupErrorDB(True)
    setupUserDB(True)
    setupEmails()


def createAdmin():
    """Create initial admin user."""
    adminEmail = CONFIG_BROKER['admin_email']
    adminPass = CONFIG_BROKER['admin_password']
    userDb = UserHandler()
    userDb.createUserWithPassword(adminEmail, adminPass, Bcrypt(), admin=True)
    user = userDb.getUserByEmail(adminEmail)
    userDb.addUserInfo(user, "Admin", "System", "System Admin")
    userDb.session.close()


def setupSessionTable():
    SessionTable.createTable(CONFIG_BROKER['local'], CONFIG_DB['dynamo_port'])


def start():
    from dataactbroker.app import runApp
    runApp()

if __name__ == '__main__':
    options()
