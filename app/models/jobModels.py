""" These classes define the ORM models to be used by sqlalchemy for the job tracker database """

import sqlalchemy
from sqlalchemy import Column, Integer, Text
from sqlalchemy.ext.declarative import declarative_base



Base = declarative_base()
class JobStatus(Base):
    __tablename__ = 'job_status'

    job_id = Column(Integer, primary_key=True)
    filename = Column(Text)
    status_id = Column(Integer)
    type_id = Column(Integer)
    resource_id = Column(Integer)

class JobDependency(Base):
    __tablename__ = 'job_dependency'

    dependency_id = Column(Integer, primary_key=True)
    job_id = Column(Integer)
    prerequisite_id = Column(Integer)

class Status(Base):
    __tablename__ = 'status'

    status_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

class Type(Base):
    __tablename__ = 'type'

    type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

class Resource(Base):
    __tablename__ = 'resource'

    resource_id = Column(Integer, primary_key=True)