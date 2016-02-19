from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.userModel import EmailTemplate, EmailTemplateType
class UserInterface(BaseInterface):
    """ Manages all interaction with the validation database

    STATIC FIELDS:
    dbName -- Name of job tracker database
    dbConfigFile -- Full path to credentials file
    """

    dbName = "user_manager"
    credFileName = "dbCred.json"
    Session = None
    engine = None
    session = None

    def __init__(self):
        self.dbConfigFile = self.getCredFilePath()
        super(UserInterface,self).__init__()

    def loadEmailTemplate(self,subject,contents,emailType):
        emailId = self.session.query(EmailTemplateType.email_template_type_id).filter(EmailTemplateType.name == emailType).one()
        template = EmailTemplate()
        template.subject = subject
        template.content = contents
        template.template_type_id = emailId
        self.session.add(template)
        self.session.commit()

    @staticmethod
    def getDbName():
        """ Return database name"""
        return UserInterface.dbName
