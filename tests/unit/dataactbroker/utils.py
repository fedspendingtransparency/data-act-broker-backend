from dataactcore.models.userModel import PermissionType, UserStatus
from dataactcore.models.lookups import PERMISSION_TYPE_DICT, USER_STATUS_DICT


def insert_permissions(db):
    for key in PERMISSION_TYPE_DICT.keys():
        db.session.add(PermissionType(name=key))
        db.session.commit()


def insert_user_statuses(db):
    for key in USER_STATUS_DICT.keys():
        db.session.add(UserStatus(name=key))
        db.session.commit()