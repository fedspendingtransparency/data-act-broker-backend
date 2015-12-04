import sqlalchemy
import sys
import os
import json
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, update
from models.userModel import User
from sqlalchemy.orm.exc import MultipleResultsFound

class UserHandler:
    dbName = "user_manager"
    credentialsFile = "dbCred.json"
    host = "localhost"
    port = "5432"

    def __init__(self):
        # Load credentials from config file
        cred = open(self.credentialsFile,"r").read()
        credDict = json.loads(cred)
        # Set up engine and session
        self.engine = create_engine("postgresql://"+credDict["username"]+":"+credDict["password"]+"@"+self.host+":"+self.port+"/"+self.dbName)
        self.connection = self.engine.connect()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def getUserId(self, username):
        # Check if user exists
        queryResult = self.session.query(User.user_id).filter(User.username == username).all()
        if(len(queryResult) == 1):
            # If so, return ID
            return queryResult[0].user_id
        elif(len(queryResult) == 0):
            # If not, add new user
            newUser = User(username = username)
            self.session.add(newUser)
            self.session.commit()
            return newUser.user_id
        else:
            # Multiple entries for this user, server error
            raise MultipleResultsFound("Multiple entries for single username")
