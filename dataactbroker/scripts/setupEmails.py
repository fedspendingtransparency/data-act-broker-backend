from dataactcore.models.userModel import EmailTemplateType, EmailTemplate

from dataactcore.interfaces.db import databaseSession


def setupEmails():
    """Create email templates from model metadata."""
    with databaseSession() as sess:

        # insert email template types
        typeList = [
            ('validate_email', ''),
            ('account_approved', ''),
            ('account_rejected', ''),
            ('reset_password', ''),
            ('account_creation', ''),
            ('account_creation_user', ''),
            ('unlock_account', ''),
            ('review_submission','')
        ]
        for t in typeList:
            emailId = sess.query(
                EmailTemplateType.email_template_type_id).filter(
                EmailTemplateType.name == t[0]).one_or_none()
            if not emailId:
                type = EmailTemplateType(name=t[0], description=t[1])
                sess.add(type)

        sess.commit()

        # insert email templates

        #Confirm
        template = "This email address was just used to create a user account with the DATA Act Broker.  To continue the registration process, please click <a href='[URL]'>here</a>. The link will expire in 24 hours. <br />  <br />  If you did not initiate this process, you may disregard this email.<br /><br />The DATA Act Broker Helpdesk<br />DATABroker@fiscal.treasury.gov "
        loadEmailTemplate(sess, "DATA Act Broker - Registration", template, "validate_email")

        #Approve
        template = "Thank you for registering for a user account with the DATA Act Broker. Your request has been approved by the DATA Act Broker Help Desk. You may now log into the Data Broker portal, using the password you created at registration, by clicking <a href='[URL]'>here</a>.<br /><br /> If you have any questions, please contact the DATA Act Broker Help Desk at [EMAIL].<br /><br />DATA Act Broker Helpdesk<br />DATABroker@fiscal.treasury.gov"
        loadEmailTemplate(sess, "DATA Act Broker - Access Approved", template, "account_approved")

        #Reject
        template = "Thank you for requesting log-in credentials for the DATA Act Broker. Your attempt to register has been denied. If you believe this determination was made in error, please contact the DATA Act Broker Helpdesk at DATABroker@fiscal.treasury.gov.<br /><br />DATA Act Broker Helpdesk<br />DATABroker@fiscal.treasury.gov"
        loadEmailTemplate(sess, "DATA Act Broker - Access Denied", template, "account_rejected")

        #Password Reset
        template = "You have requested your password to be reset for your account. Please click the following link <a href='[URL]'>here</a> to start the processs. The link will expire in 24 hours. <br/> <br/> If you did not request this password reset, please notify the DATA Act Broker Helpdesk (DATABroker@fiscal.treasury.gov) <br /><br />DATA Act Broker Helpdesk<br /><br />DATABroker@fiscal.treasury.gov"
        loadEmailTemplate(sess, "DATA Act Broker - Password Reset", template, "reset_password")

        #Admin Email
        template = "This email is to notify you that the following person has requested an account for the DATA Act Broker:<br /><br />Name: [REG_NAME]<br /><br />Title:  [REG_TITLE]<br /><br />Agency Name:  [REG_AGENCY_NAME]<br /><br />CGAC Code: [REG_CGAC_CODE]<br /><br />Email: [REG_EMAIL]<br /><br /><br /><br />To approve or deny this user for access to the Data Broker, please click <a href='[URL]'>here</a>.<br /><br />This action must be taken within 24 hours. <br /><br />Thank you for your prompt attention.<br /><br />DATA Act Broker Helpdesk<br />DATABroker@fiscal.treasury.gov"
        loadEmailTemplate(sess, "New Data Broker registration - Action Required", template, "account_creation")

        #User Email When finished submitting
        template = ("Thank you for registering a DATA Act Broker user account. "
            "The final registration step is for the Help Desk to review your "
            "request. You should receive an e-mail update from them within one "
            "business day, saying whether they've approved or denied your access."
            "<br /><br />"
            "Until the Help Desk approves your request, you won't be able to log "
            "into the Broker. Thanks for being patient with the security process--"
            "we appreciate your interest and look forward to working with you."
            "<br /><br/>"
            "If you have any questions or haven't received a follow-up e-mail "
            "within one business day, please get in touch with the Help Desk at "
            "[EMAIL]."
            "<br /><br />"
            "The DATA Act Implementation Team <br />"
            "[EMAIL]")
        loadEmailTemplate(sess, "DATA Act Broker - Registration", template, "account_creation_user")

        #Unlock account email
        template = "Your account has been unlocked and requires your password to be reset. Please click the following link <a href='[URL]'>here</a> to start the processs. The link will expire in 24 hours. <br/> <br/> If you did not request your account to be unlocked, please notify the DATA Act Broker Helpdesk (DATABroker@fiscal.treasury.gov) <br /><br />DATA Act Broker Helpdesk<br /><br />DATABroker@fiscal.treasury.gov"
        loadEmailTemplate(sess, "DATA Act Broker - Password Reset", template, "unlock_account")

        #Submission Review
        template = "[REV_USER_NAME] has shared a DATA Act broker submission with you. Click <a href='[REV_URL]'>here</a> to review their submission. For questions or comments, please email the DATA Act Broker Helpdesk (DATABroker@fiscal.treasury.gov)."
        loadEmailTemplate(sess, "DATA Act Broker - Submission Ready for Review", template, "review_submission")


def loadEmailTemplate(sess, subject, contents, emailType):
    """ Upsert a broker e-mail template.

    Args:
        sess - Database session
        subject - Subject line
        contents - Body of email, can include tags to be replaced
        emailType - Type of template, if there is already an entry for this type it will be overwritten
    """
    emailId = sess.query(
        EmailTemplateType.email_template_type_id).filter(
        EmailTemplateType.name == emailType).one()
    templateId = sess.query(
        EmailTemplate.email_template_id).filter(
        EmailTemplate.template_type_id == emailId).one_or_none()
    template = EmailTemplate()
    if templateId:
        template.email_template_id = templateId
    template.subject = subject
    template.content = contents
    template.template_type_id = emailId
    sess.merge(template)
    sess.commit()

if __name__ == '__main__':
    setupEmails()
