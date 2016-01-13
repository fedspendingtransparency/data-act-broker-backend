""" These classes define the ORM models to be used by sqlalchemy for the job tracker database """

from sqlalchemy import Column, Integer, Text, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class FileType(Base):
    __tablename__ = "file_type"

    file_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

class FieldType(Base):
    __tablename__ = "field_type"

    field_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

class RuleType(Base):
    __tablename__ = "rule_type"

    rule_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

    session = None
    TYPE_DICT = None
    TYPE_LIST = ["TYPE", "EQUAL","NOT EQUAL","LESS","GREATER","LENGTH"]

    @staticmethod
    def getType(typeName):
        if(RuleType.TYPE_DICT == None):
            RuleType.TYPE_DICT = {}
            # Pull status values out of DB
            for type in RuleType.TYPE_LIST:
                RuleType.TYPE_DICT[type] = RuleType.setType(type)
        if(not typeName in RuleType.TYPE_DICT):
            raise ValueError("Not a valid job type")
        return RuleType.TYPE_DICT[typeName]

    @staticmethod
    def setType(name):
        """  Get an id for specified type, if not unique throw an exception

        Arguments:
        name -- Name of type to get an id for

        Returns:
        type_id of the specified type
        """
        if(RuleType.session == None):
            from dataactcore.models.validationInterface import ValidationInterface
            RuleType.session = ValidationInterface().getSession()
        queryResult = RuleType.session.query(RuleType.rule_type_id).filter(RuleType.name==name).all()
        RuleType.session.close()
        if(len(queryResult) != 1):
            # Did not get a unique result
            raise ValueError("Database does not contain a unique ID for type "+name)
        else:
            return queryResult[0].rule_type_id

class FileColumn(Base):
    __tablename__ = "file_columns"

    file_column_id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey("file_type.file_id"), nullable=True)
    file = relationship("FileType", uselist=False)
    field_types_id = Column(Integer, ForeignKey("field_type.field_type_id"), nullable=True)
    field_type = relationship("FieldType", uselist=False)
    name = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    required = Column(Boolean, nullable=True)


class Rule(Base):
    __tablename__ = "rule"
    rule_id = Column(Integer, primary_key=True)
    file_column_id = Column(Integer, ForeignKey("file_columns.file_column_id"), nullable=True)
    rule_type_id  = Column(Integer, ForeignKey("rule_type.rule_type_id"), nullable=True)
    rule_text_1 = Column(Text, nullable=True)
    rule_text_2 = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    rule_type = relationship("RuleType", uselist=False)
    file_column = relationship("FileColumn", uselist=False)
