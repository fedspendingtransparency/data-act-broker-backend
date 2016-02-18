""" These classes define the ORM models to be used by sqlalchemy for the user database """

from sqlalchemy import Column, Integer, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
class User(Base):
    __tablename__ = 'users'

    user_id = Column(Integer, primary_key=True)
    username = Column(Text)
    name = Column(Text)
    agency = Column(Text)
    title = Column(Text)
    user_status_id = Column(Integer, ForeignKey("user_status.user_status_id"))

class UserStatus(Base):
    __tablename__ = 'user_status'

    user_status_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)