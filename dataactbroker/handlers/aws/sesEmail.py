import boto
import datetime
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.function_bag import get_email_template


class SesEmail(object):
    """ An email object that can be sent to a user

        Attributes:
            to_address: A string containing the address to send the email to
            from_address: A string containing the address the email is being sent from
            subject: A string containing the subject line of the email
            content: A string containing the content of the email

        Class Attributes:
            is_local: A boolean indicating if the application is being run locally or not
            emailLog: A string indicating the name of the email log file for local emails

        Constants:
            SIGNING_KEY: A string containing the signing key
    """

    # todo: is SIGNING_KEY something that should live in the config file?
    # TODO: See if this is even needed and, if not, remove it from here, the config file, and the app file
    SIGNING_KEY = "1234"
    is_local = False
    emailLog = "Email.log"

    def __init__(self, to_address, from_address, content="", subject="", template_type=None, parameters=None):
        """ Creates an email object to be sent

            Args:
                to_address: Email is sent to this address
                from_address: This will appear as the sender, must be an address verified through S3 for cloud version
                content: Body of email
                subject: Subject line of email
                template_type: What type of template to use to fill in the email
                parameters: Dict of replacement values to populate the template
        """
        self.to_address = to_address
        self.from_address = from_address
        if template_type is None:
            self.content = content
            self.subject = subject
        else:
            template = get_email_template(template_type)
            self.subject = template.subject
            self.content = template.content

            for key in parameters:
                if parameters[key] is not None:
                    self.content = self.content.replace(key, parameters[key])
                else:
                    self.content = self.content.replace(key, "")

    def send(self):
        """ Send the email built in the constructor """
        if not SesEmail.is_local:
            # Use aws creds for ses if possible, otherwise, use aws_key from config
            connection = boto.connect_ses()
            try:
                return connection.send_email(self.from_address, self.subject, self.content,
                                             self.to_address, format='html')
            except:
                connection = boto.connect_ses(aws_access_key_id=CONFIG_BROKER['aws_access_key_id'],
                                              aws_secret_access_key=CONFIG_BROKER['aws_secret_access_key'])
                return connection.send_email(self.from_address, self.subject, self.content,
                                             self.to_address, format='html')
        else:
            new_email_text = "\n\n".join(["", "Time", str(datetime.datetime.now()), "Subject", self.subject, "From",
                                         self.from_address, "To", self.to_address, "Content", self.content])
            open(SesEmail.emailLog, "a").write(new_email_text)
