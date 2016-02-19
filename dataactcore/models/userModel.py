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


class EmailTemplateType(Base):
    __tablename__ = 'email_template_type'
    email_template_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

class EmailTemplate(Base):
    __tablename__ = 'email_template'

    email_template_id = Column(Integer, primary_key=True)
    template_type_id = Column(Integer, ForeignKey("email_template_type.email_template_type_id"))
    subject = Column(Text)
    content = Column(Text)

class EmailToken(Base):
    __tablename__ = 'email_token'
    email_token_id = Column(Integer, primary_key=True)
    token = Column(Text)
    salt = Column(Text)
