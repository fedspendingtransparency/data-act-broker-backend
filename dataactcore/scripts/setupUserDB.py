from dataactcore.scripts.databaseSetup import runCommands
from dataactcore.models.userInterface import UserInterface

def setupUserDB(hardReset = False):
    if(hardReset):
        sql = [
            "DROP TABLE IF EXISTS users",
            "DROP SEQUENCE IF EXISTS userIdSerial",
            "DROP TABLE IF EXISTS user_status",
            "DROP TABLE IF EXISTS email_template",
            "DROP TABLE IF EXISTS email_template_type",
            "DROP TABLE IF EXISTS email_token",
            "DROP SEQUENCE IF EXISTS emailTemplateSerial",
            "DROP SEQUENCE IF EXISTS emailTokenSerial",
            "DROP TABLE IF EXISTS permission_type"
            ]
        runCommands(UserInterface.getCredDict(),sql,"user_manager")

    sql = [
            "DROP SEQUENCE IF EXISTS userIdSerial",
            "CREATE TABLE user_status (user_status_id integer PRIMARY KEY, name text, description text)",
            "CREATE TABLE permission_type (permission_type_id integer PRIMARY KEY, name text, description text)",
            "CREATE SEQUENCE userIdSerial START 1",
            "CREATE TABLE users (user_id integer PRIMARY KEY DEFAULT nextval('userIdSerial'), username text, email text, password_hash text, name text, agency text, title text, user_status_id integer REFERENCES user_status, permissions integer)",
            "INSERT INTO user_status (user_status_id, name, description) VALUES (1, 'awaiting_confirmation', 'User has entered email but not confirmed'), (2, 'email_confirmed', 'User email has been confirmed'), (3, 'awaiting_approval', 'User has registered their information and is waiting for approval'), (4, 'approved', 'User has been approved'), (5, 'denied','User registration was denied')",
            "CREATE SEQUENCE emailTemplateSerial START 1",
            ("CREATE TABLE email_template("
                "email_template_id integer PRIMARY KEY DEFAULT nextval('emailTemplateSerial'),"
                "subject text,"
                "content text,"
                "template_type_id integer NOT NULL"
            ")"),
            "CREATE SEQUENCE emailTokenSerial START 1",
            "CREATE TABLE email_token (email_token_id integer PRIMARY KEY DEFAULT nextval('emailTokenSerial'), token text, salt text)",
            "CREATE TABLE email_template_type (email_template_type_id integer PRIMARY KEY, name text, description text)",
            ("INSERT INTO email_template_type (email_template_type_id, name, description) VALUES"
                "(1, 'validate_email', 'Email to confirm email address'),"
                "(2, 'account_creation', 'Email to notify admin of account request'),"
                "(3, 'account_approved', 'Email to notify user of the successful account creation'),"
                "(4, 'account_rejected', ' Email to notify user of the unsuccessful account creation'),"
                "(5, 'reset_password', ' Email to notify allow users to reset the password')"),
            ("INSERT INTO permission_type (permission_type_id, name, description) VALUES"
                "(1, 'agency_user', 'This user is allowed to upload data to be validated'),"
                "(2, 'website_admin', 'This user is allowed to manage user accounts')")
           ]
    runCommands(UserInterface.getCredDict(),sql,"user_manager")
    database = UserInterface()

    #Confirm
    template = "Hello  [USER],</br></br>To confirm your email address for the DATA Act Broker please click [URL]. The link will expire in 24 hours.</br></br>Thank you,</br></br>DATA Act Broker Admin Team"
    database.loadEmailTemplate("DATA Act Broker : Account Email Confirmation ",template,"validate_email")

    #Approve
    template = "Hello  [USER],</br></br>You have been approved to use the DATA Act Broker which can be reached here [URL].</br></br>Thank you,\nDATA Act Broker Admin"
    database.loadEmailTemplate("DATA Act Broker : Account Approved ",template,"account_approved")

    #Reject
    template = "Hello  [USER],\n\t You have been disapproved to use the DATA Act Broker. The following reason was provided:[REASON] \nThank you,\nDATA Act Broker Admin"
    database.loadEmailTemplate("DATA Act Broker : Account Rejected ",template,"account_rejected")

    #Password Reset
    template = "Hello  [USER],\n\t You have requested your password to be reset for your account. Please click the following link [URL] to start the processs.The link will expire in 24 hours \nThank you,\nDATA Act Broker Admin"
    database.loadEmailTemplate("DATA Act Broker : Password Reset ",template,"reset_password")

    #Admin Email
    template = "Hello  [USER],\n\t The following user [USER2] has requested and account. Please login to reject or approve the user.  \nThank you,\nDATA Act Broker System"
    database.loadEmailTemplate("DATA Act Broker : Account Pending Approval ",template,"reset_password")

if __name__ == '__main__':
    setupUserDB(hardReset = True)
