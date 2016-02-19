import boto
import uuid
import urllib
from flask import url_for
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound
from dataactbroker.handlers.userHandler import UserHandler
class sesEmail(object):

    #TODO Make JSON
    SIGNING_KEY  ="12345"

    def __init__(self,toAddress,fromAddress,content,subject):
        self.toAddress = toAddress
        self.fromAddress = fromAddress
        self.content = content
        self.subject = subject

    def send(self):
        connection = boto.connect_ses()
        return connection.send_email(self.fromAddress, self.subject,self.content,self.toAddress,format='html')


    @staticmethod
    def createToken(emailAddress,database) :
        """Creates a token to be used and saves it with the salt in the database"""
        salt = uuid.uuid1().int
        ts = URLSafeTimedSerializer(sesEmail.SIGNING_KEY)
        token = ts.dumps(emailAddress, salt=str(salt))
        #saves the token and salt pair
        database.saveToken(str(salt),str(token))
        Wait(2)
        return urllib.quote_plus(str(token))

    @staticmethod
    def checkToken(token,database):
        """Gets token's salt and decodes it"""
        saltValue = None
        try:
            saltValue = database.getTokenSalt(token)
        except MultipleResultsFound, e:
            #duplicate tokens
            return False ,""
        except NoResultFound, e:
            #Token allready used or never existed in the first place
            return False,""
        ts = URLSafeTimedSerializer(sesEmail.SIGNING_KEY)
        try:
            emailAddress = ts.loads(token, salt=saltValue[0], max_age=1)
            return True,emailAddress
        except BadSignature:
            #Token is malformed
            return False,""
        except SignatureExpired:
            #Token is to old
            return False,""
