""" These classes define the ORM models to be used by sqlalchemy for the error database """

from sqlalchemy import Column, Integer, Text, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
class Status(Base):
    __tablename__ = "status"
    STATUS_DICT = None
    session = None

    status_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

    @staticmethod
    def getStatus(statusName):
        if(Status.STATUS_DICT == None or (len(Status.STATUS_DICT)==0)):
            Status.STATUS_DICT = {}
            # Pull status values out of DB
            if(Status.session == None):
                from dataactcore.models.errorInterface import ErrorInterface
                errorDB = ErrorInterface()
                Status.session = errorDB.getSession()
            queryResult = Status.session.query(Status).all()

            for status in queryResult:
                Status.STATUS_DICT[status.name] = status.status_id
            Status.session.close()
        if(not statusName in Status.STATUS_DICT):
            open("errorLog","a").write("Not a valid file status: " + statusName + ", dict is: " + str(Status.STATUS_DICT))
            raise ValueError("Not a valid file status: " + statusName + ", dict is: " + str(Status.STATUS_DICT))
        return Status.STATUS_DICT[statusName]

class ErrorType(Base):
    __tablename__ = "error_type"
    TYPE_DICT = None
    session = None

    error_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

    @staticmethod
    def getType(typeName):
        if(ErrorType.TYPE_DICT == None):
            ErrorType.TYPE_DICT = {}
            # Pull status values out of DB
            if(ErrorType.session == None):
                from dataactcore.models.errorInterface import ErrorInterface
                ErrorType.session = ErrorInterface().getSession()
            queryResult = ErrorType.session.query(ErrorType).all()

            for type in queryResult:
                ErrorType.TYPE_DICT[type.name] = type.error_type_id
            ErrorType.session.close()
        if(not typeName in ErrorType.TYPE_DICT):
            raise ValueError("Not a valid error type: " + typeName)
        return ErrorType.TYPE_DICT[typeName]

class FileStatus(Base):
    __tablename__ = "file_status"

    file_id = Column(Integer, primary_key=True)
    job_id = Column(Integer, nullable=True)
    filename = Column(Text, nullable=True)
    status_id = Column(Integer, ForeignKey("status.status_id"))
    status = relationship("Status", uselist=False)

class ErrorData(Base):
    __tablename__ = "error_data"

    error_data_id = Column(Integer, primary_key=True)
    job_id = Column(Integer)
    filename = Column(Text, nullable=True)
    field_name = Column(Text)
    error_type_id = Column(Integer, ForeignKey("error_type.error_type_id"), nullable=True)
    error_type = relationship("ErrorType", uselist=False)
    occurrences = Column(Integer)
    first_row = Column(Integer)
    rule_failed = Column(Text, nullable=True)

