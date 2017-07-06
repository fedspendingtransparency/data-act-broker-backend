""" These classes define the ORM models to be used by sqlalchemy for the user database """

from sqlalchemy import Column, Integer, Text, ForeignKey, Boolean, Index
from sqlalchemy.orm import relationship
from dataactcore.models.baseModel import Base
from dataactcore.models.lookups import PERMISSION_TYPE_DICT_ID


class User(Base):
    __tablename__ = 'users'

    user_id = Column(Integer, primary_key=True)
    username = Column(Text)
    email = Column(Text)
    name = Column(Text)
    title = Column(Text)
    password_hash = Column(Text)
    salt = Column(Text)
    skip_guide = Column(Boolean, default=False, nullable=False, server_default="False")
    website_admin = Column(Boolean, default=False, nullable=False, server_default="False")
    affiliations = relationship("UserAffiliation", cascade="all, delete-orphan")


class PermissionType(Base):
    __tablename__ = "permission_type"

    permission_type_id = Column(Integer, primary_key=True)
    name = Column(Text)
    description = Column(Text)


class UserAffiliation(Base):
    __tablename__ = 'user_affiliation'
    # composite primary_key
    user_id = Column(Integer, ForeignKey("users.user_id", name="user_affiliation_user_fk", ondelete='CASCADE'),
                     primary_key=True)
    cgac_id = Column(Integer, ForeignKey("cgac.cgac_id", name="user_affiliation_cgac_fk", ondelete='CASCADE'),
                     primary_key=True)
    frec_id = Column(Integer, ForeignKey("frec.frec_id", name="user_affiliation_frec_fk", ondelete='CASCADE'),
                     primary_key=True)
    cgac = relationship("CGAC")
    frec = relationship("FREC")
    permission_type_id = Column(
        Integer, ForeignKey(column="permission_type.permission_type_id", name="user_affiliation_permission_type_fk"))

    @property
    def permission_type_name(self):
        return PERMISSION_TYPE_DICT_ID.get(self.permission_type_id)


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

Index("ix_session_uid", SessionMap.uid, unique=False)
