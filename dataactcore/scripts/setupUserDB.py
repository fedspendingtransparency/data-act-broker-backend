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
            "CREATE TABLE users (user_id integer PRIMARY KEY DEFAULT nextval('userIdSerial'),username text, email text, password_hash text, name text, agency text, title text, user_status_id integer REFERENCES user_status, permissions integer, salt text)",
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
                "(5, 'reset_password', ' Email to notify allow users to reset the password'),"
                "(6, 'account_creation_user', ' Email to notify allow users to reset the password')"),
            ("INSERT INTO permission_type (permission_type_id, name, description) VALUES"
                "(0, 'agency_user', 'This user is allowed to upload data to be validated'),"
                "(1, 'website_admin', 'This user is allowed to manage user accounts')")
           ]
    runCommands(UserInterface.getCredDict(),sql,"user_manager")
    database = UserInterface()

    #Confirm
    template = "This email address was just used to create a user account with the USA Spending Data Broker.  To continue the registration process, please click [URL]. The link will expire in 24 hours. <br />  <br />  If you did not initiate this process, you may disregard this email.<br /><br />The DATA Act Implementation Team<br />DATAPMO@fiscal.treasury.gov "
    database.loadEmailTemplate("USA Spending Data Broker - Registration",template,"validate_email")

    #Approve
    template = "Thank you for registering for a user account with the USA Spending Data Broker. Your request has been approved by your agency's administrator. You may now log into the Data Broker portal, using the password you created at registration, by clicking [URL].<br /><br /> If you have any questions, please contact your agency administrator at [EMAIL].<br /><br />The DATA Act Implementation Team<br />DATAPMO@fiscal.treasury.gov"
    database.loadEmailTemplate("USA Spending Data Broker - Access Approved",template,"account_approved")

    #Reject
    template = "Thank you for requesting log-in credentials for the USA Spending Data Broker. Your attempt to register has been denied. If you believe this determination was made in error, please contact the USA Spending Project Management Office at DATAPMO@fiscal.treasury.gov.<br /><br />The DATA Act Implementation Team<br />DATAPMO@fiscal.treasury.gov"
    database.loadEmailTemplate("USA Spending Data Broker - Access Denied",template,"account_rejected")

    #Password Reset
    template = "You have requested your password to be reset for your account. Please click the following link [URL] to start the processs. The link will expire in 24 hours. <br /><br />The DATA Act Implementation Team <br /><br />DATAPMO@fiscal.treasury.gov"
    database.loadEmailTemplate("USA Spending Data Broker - Password Reset",template,"reset_password")

    #Admin Email
    template = "This email is to notify you that the following person has requested an account for the USA Spending Data Broker:<br /><br />Name: [REG_NAME]<br /><br />Title:  [REG_TITEL]<br /><br />Agency:  [REG_AGENCY]<br /><br />Email: [REG_EMAIL]<br /><br /><br /><br />To approve or deny this user for access to the Data Broker, please click [URL].<br /><br />This action must be taken within 24 hours. <br /><br />Thank you for your prompt attention.<br /><br />The DATA Act Implementation Team<br />DATAPMO@fiscal.treasury.gov"
    database.loadEmailTemplate("New Data Broker registration - Action Required",template,"account_creation")

    #User Email When finished submitting
    template = "Thank you for registering for a user account with the USA Spending Data Broker. Your information has been sent to your agency's administrator at [EMAIL]. You will receive a response within 24 hours. <br /><br />The DATA Act Implementation Team <br />DATAPMO@fiscal.treasury.gov "
    database.loadEmailTemplate("USA Spending Data Broker - Registration",template,"account_creation_user")



if __name__ == '__main__':
    setupUserDB(hardReset = True)
