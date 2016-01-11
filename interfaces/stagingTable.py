from sqlalchemy.exc import ProgrammingError, ResourceClosedError
from sqlalchemy.ext.declarative import declarative_base
from dataactcore.models.baseInterface import BaseInterface
from interfaces.interfaceHolder import InterfaceHolder

class StagingTable(object):

    BATCH_INSERT = True
    INSERT_BY_ORM = False
    BATCH_SIZE = 1000

    def _init_(self, orm, name, jobId):
        self.name = name
        self.orm = orm
        # Start first batch
        self.batch = []
        self.interface = InterfaceHolder.STAGING
        self.jobId = jobId

    def create(self,engine) :
        self.orm.__table__.create(engine)

    def endBatch(self):
        """ Called at end of process to send the last batch """
        if(len(self.batch)>0):
            self.interface.connection(self.orm.__table__.insert(),self.batch)
            self.batch = []
            return True
        else:
            return False

    def insert(self, record):
        if(self.BATCH_INSERT):
            if(self.INSERT_BY_ORM):
                raise NotImplementedError("Have not implemented ORM method for batch insert")
            else:
                self.batch.append(record)
                if(len(self.batch)>self.BATCH_SIZE):
                    # Time to write the batch
                    self.interface.connection(self.orm.__table__.insert(),self.batch)
                    # Reset batch
                    self.batch = []
                return True
        else:
            if(self.INSERT_BY_ORM):
                try:
                    recordOrm = self.orm()
                except:
                    # createTable was not called
                    raise Exception("Must call createTable before writing")

                attributes = self.getPublicMembers(recordOrm)

                # For each field, add value to ORM object
                for key in record.iterkeys():
                    attr = key.replace(" ","_")
                    setattr(recordOrm,attr,record[key])

                self.interface.session.add(recordOrm)
                self.interface.session.commit()
                return True
            else:
                raise ValueError("Must do either batch or use ORM, cannot set both to False")

    @staticmethod
    def getPublicMembers(obj):
        response = []
        for member in dir(obj):
            if(member[0] != "_"):
                response.append(member)
        return response
