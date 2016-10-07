import boto
import uuid
import urllib.parse
import datetime
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.userModel import EmailToken


class sesEmail(object):

    # todo: is SIGNING_KEY something that should live in the config file?
    SIGNING_KEY = "1234"
    INVALID_LINK = 1
    LINK_EXPIRED = 2
    LINK_ALREADY_USED = 3
    LINK_VALID = 0
    isLocal = False
    emailLog = "Email.log"

    def __init__(self,toAddress,fromAddress,content="",subject="",templateType=None,parameters=None, database=None):
        """ Creates an email object to be sent
        Args:
            toAddress: Email is sent to this address
            fromAddress: This will appear as the sender, must be an address verified through S3 for cloud version
            content: Body of email
            subject: Subject line of email
            templateType: What type of template to use to fill in the email
            parameters: Dict of replacement values to populate the template
            database: Interface object to User DB
        """
        self.toAddress = toAddress
        self.fromAddress = fromAddress
        if(templateType is None):
            self.content = content
            self.subject = subject
        else:
            template = database.getEmailTemplate(templateType)
            self.subject = template.subject
            self.content = template.content

            for key in parameters :
                if parameters[key] is not None:
                    self.content = self.content.replace(key,parameters[key])
                else:
                    self.content = self.content.replace(key,"")

    def send(self):
        """ Send the email built in the constructor """
        if(not sesEmail.isLocal):
            # Use aws creds for ses if possible, otherwise, use aws_key from config
            connection = boto.connect_ses()
            try:
                return connection.send_email(self.fromAddress, self.subject,self.content,self.toAddress,format='html')
            except:
                connection = boto.connect_ses(aws_access_key_id=CONFIG_BROKER['aws_access_key_id'], aws_secret_access_key=CONFIG_BROKER['aws_secret_access_key'])
                return connection.send_email(self.fromAddress, self.subject,self.content,self.toAddress,format='html')
        else:
            newEmailText = "\n\n".join(["","Time",str(datetime.datetime.now()),"Subject",self.subject,"From",self.fromAddress,"To",self.toAddress,"Content",self.content])
            open (sesEmail.emailLog,"a").write(newEmailText)


    @staticmethod
    def createToken(emailAddress, token_type):
        """Creates a token and saves it with the salt in the database."""
        sess = GlobalDB.db().session
        salt = str(uuid.uuid1().int)
        ts = URLSafeTimedSerializer(sesEmail.SIGNING_KEY)
        token = ts.dumps(emailAddress, salt=salt + token_type)
        # save the token and salt pair
        newToken = EmailToken(salt=salt, token=token)
        sess.add(newToken)
        sess.commit()
        return urllib.parse.quote_plus(str(token))


    @staticmethod
    def checkToken(token,database,token_type):
        """Gets token's salt and decodes it"""
        saltValue = None
        try:
            saltValue = database.getTokenSalt(token)
        except MultipleResultsFound as e:
            #duplicate tokens
            return False ,"Invalid Link", sesEmail.INVALID_LINK
        except NoResultFound as e:
            #Token already used or never existed in the first place
            return False,"Link already used",sesEmail.LINK_ALREADY_USED
        ts = URLSafeTimedSerializer(sesEmail.SIGNING_KEY)
        try:
            emailAddress = ts.loads(token, salt=saltValue[0]+token_type, max_age=86400)
            return True,emailAddress,sesEmail.LINK_VALID
        except BadSignature as e:
            #Token is malformed
            return False,"Invalid Link",sesEmail.INVALID_LINK
        except SignatureExpired:
            #Token is to old
            return False,"Link Expired",sesEmail.LINK_EXPIRED
