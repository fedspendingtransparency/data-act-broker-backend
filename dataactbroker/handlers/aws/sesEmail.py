import boto
import uuid
import urllib
import datetime
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound

class sesEmail(object):

    SIGNING_KEY  ="1234"
    INVALID_LINK  = 1
    LINK_EXPIRED =  2
    LINK_ALREADY_USED = 3
    LINK_VALID  = 0
    isLocal = False
    emailLog = "Email.log"
    def __init__(self,toAddress,fromAddress,content="",subject="",templateType=None,parameters=None, database=None):
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
                if(not parameters[key] is None) :
                    self.content = self.content.replace(key,parameters[key])
                else :
                    self.content = self.content.replace(key,"")

    def send(self):
        if(not sesEmail.isLocal):
            connection = boto.connect_ses()
            return connection.send_email(self.fromAddress, self.subject,self.content,self.toAddress,format='html')
        else:
            newEmailText = "\n\n".join(["","Time",str(datetime.datetime.now()),"Subject",self.subject,"From",self.fromAddress,"To",self.toAddress,"Content",self.content])
            open (sesEmail.emailLog,"a").write(newEmailText)


    @staticmethod
    def createToken(emailAddress,database,token_type) :
        """Creates a token to be used and saves it with the salt in the database"""
        salt = uuid.uuid1().int
        ts = URLSafeTimedSerializer(sesEmail.SIGNING_KEY)
        token = ts.dumps(emailAddress, salt=str(salt)+token_type)
        #saves the token and salt pair
        database.saveToken(str(salt),str(token))
        return urllib.quote_plus(str(token))

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
