from dataactcore.scripts.databaseSetup import runCommands
from dataactcore.models.userInterface import UserInterface

def setupJobTrackerDB(hardReset = False):
    if(hardReset):
        sql = ["DROP TABLE IF EXISTS users","DROP SEQUENCE IF EXISTS userIdSerial"]
        runCommands(UserInterface.getCredDict(),sql,"user_manager")

    sql = ["DROP SEQUENCE IF EXISTS userIdSerial",
            "CREATE TABLE user_status (user_status_id integer PRIMARY KEY, name text, description text",
            "CREATE SEQUENCE userIdSerial START 1",
            "CREATE TABLE users (user_id integer PRIMARY KEY DEFAULT nextval('userIdSerial'), username text, email text, password_hash text, name text, agency text, title text, status integer REFERENCES user_status)",
            "INSERT INTO user_status (user_status_id, name, description) VALUES (1, 'awaiting_confirmation', 'User has entered email but not confirmed'), (2, 'email_confirmed', 'User's email has been confirmed), (3, 'awaiting_approval', 'User has registered their information and is waiting for approval'), (4, 'approved', 'User has been approved'), (5, 'denied','User registration was denied')"
           ]

    runCommands(UserInterface.getCredDict(),sql,"user_manager")



if __name__ == '__main__':
    setupJobTrackerDB(hardReset = True)
