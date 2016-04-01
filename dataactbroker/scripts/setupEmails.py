from dataactbroker.handlers.userHandler import UserHandler
from dataactbroker.handlers.interfaceHolder import InterfaceHolder




def setupEmails():

    database = UserHandler()
    #Confirm
    template = "This email address was just used to create a user account with the USA Spending Data Broker.  To continue the registration process, please click <a href='[URL]'>here</a>. The link will expire in 24 hours. <br />  <br />  If you did not initiate this process, you may disregard this email.<br /><br />The DATA Act Implementation Team<br />DATAPMO@fiscal.treasury.gov "
    database.loadEmailTemplate("USA Spending Data Broker - Registration",template,"validate_email")

    #Approve
    template = "Thank you for registering for a user account with the USA Spending Data Broker. Your request has been approved by your agency's administrator. You may now log into the Data Broker portal, using the password you created at registration, by clicking <a href='[URL]'>here</a>.<br /><br /> If you have any questions, please contact your agency administrator at [EMAIL].<br /><br />The DATA Act Implementation Team<br />DATAPMO@fiscal.treasury.gov"
    database.loadEmailTemplate("USA Spending Data Broker - Access Approved",template,"account_approved")

    #Reject
    template = "Thank you for requesting log-in credentials for the USA Spending Data Broker. Your attempt to register has been denied. If you believe this determination was made in error, please contact the USA Spending Project Management Office at DATAPMO@fiscal.treasury.gov.<br /><br />The DATA Act Implementation Team<br />DATAPMO@fiscal.treasury.gov"
    database.loadEmailTemplate("USA Spending Data Broker - Access Denied",template,"account_rejected")

    #Password Reset
    template = "You have requested your password to be reset for your account. Please click the following link <a href='[URL]'>here</a> to start the processs. The link will expire in 24 hours. <br/> <br/> If you did not request this password reset, please notify the DATA Act PMO (DATAPMO@fiscal.treasury.gov) <br /><br />The DATA Act Implementation Team <br /><br />DATAPMO@fiscal.treasury.gov"
    database.loadEmailTemplate("USA Spending Data Broker - Password Reset",template,"reset_password")


    #Admin Email
    template = "This email is to notify you that the following person has requested an account for the USA Spending Data Broker:<br /><br />Name: [REG_NAME]<br /><br />Title:  [REG_TITEL]<br /><br />Agency:  [REG_AGENCY]<br /><br />Email: [REG_EMAIL]<br /><br /><br /><br />To approve or deny this user for access to the Data Broker, please click <a href='[URL]'>here</a>.<br /><br />This action must be taken within 24 hours. <br /><br />Thank you for your prompt attention.<br /><br />The DATA Act Implementation Team<br />DATAPMO@fiscal.treasury.gov"
    database.loadEmailTemplate("New Data Broker registration - Action Required",template,"account_creation")

    #User Email When finished submitting
    template = "Thank you for registering for a user account with the USA Spending Data Broker. Your information has been sent to your agency's administrator at [EMAIL]. You will receive a response within 24 hours. <br /><br />The DATA Act Implementation Team <br />DATAPMO@fiscal.treasury.gov "
    database.loadEmailTemplate("USA Spending Data Broker - Registration",template,"account_creation_user")
    InterfaceHolder.closeOne(database)

if __name__ == '__main__':
    setupEmails()
