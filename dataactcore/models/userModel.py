""" These classes define the ORM models to be used by sqlalchemy for the user database """

from sqlalchemy import Column, Integer, Text, ForeignKey, DateTime, Boolean
from sqlalchemy.orm import relationship
from dataactcore.models.baseModel import Base

class User(Base):
    __tablename__ = 'users'

    user_id = Column(Integer, primary_key=True)
    username = Column(Text)
    email = Column(Text)
    name = Column(Text)
    cgac_code = Column(Text)
    title = Column(Text)
    permissions = Column(Integer)
    user_status_id = Column(Integer, ForeignKey("user_status.user_status_id"))
    password_hash = Column(Text)
    salt = Column(Text)
    user_status = relationship("UserStatus", uselist=False)
    last_login_date = Column(DateTime)
    is_active = Column(Boolean, default=True, nullable=False, server_default="True")
    incorrect_password_attempts = Column(Integer, default=0, nullable=False, server_default='0')
    skip_guide = Column(Boolean, default=False,nullable=False,server_default="False")

class PermissionType(Base):
    __tablename__ = "permission_type"

    permission_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)


class UserStatus(Base):
    __tablename__ = 'user_status'
    STATUS_DICT = None

    user_status_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)

class AccountType:
    AGENCY_USER = 1
    WEBSITE_ADMIN = 2

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

class SessionMap(Base):
    """ This table maps session IDs to user data """
    __tablename__ = "session_map"
    session_id = Column(Integer, primary_key=True)
    uid = Column(Text)
    data = Column(Text)
    expiration = Column(Integer)
