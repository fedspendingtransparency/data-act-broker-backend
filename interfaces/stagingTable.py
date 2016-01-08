from sqlalchemy.exc import ProgrammingError, ResourceClosedError
from sqlalchemy.ext.declarative import declarative_base
from dataactcore.models.baseInterface import BaseInterface
class StagingTable(object):

    def _init_(orm,name,jobId):
        self.name = name
        self.orm = orm
        self.jobId

    def create(engine) :
        self.orm.__table__.create(engine)


    def insert(record):
        insertModel = self.orm()
        # Create ORM object from class defined by createTable
        attributes = self.getPublicMembers(insertModel)
        # For each field, add value to ORM object
        for key in record.iterkeys():
            attr = key.replace(" ","_")
            setattr(recordOrm,attr,record[key])
        return insertModel

    @staticmethod
    def getPublicMembers(obj):
        response = []
        for member in dir(obj):
            if(member[0] != "_"):
                response.append(member)
        return response
