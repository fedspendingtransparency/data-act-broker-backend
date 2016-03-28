from dataactcore.models import userModel
from dataactcore.models.userModel import PermissionType, UserStatus
from dataactcore.models.userInterface import UserInterface
from dataactcore.scripts.databaseSetup import createDatabase
from dataactcore.config import CONFIG_DB

def setupUserDB(hardReset = False):
    """Create user tables from model metadata."""
    createDatabase(CONFIG_DB['user_db_name'])
    userDb = UserInterface()
    if hardReset:
        userModel.Base.metadata.drop_all(userDb.engine)
    userModel.Base.metadata.create_all(userDb.engine)

    # TODO: define these codes as enums in the data model?

    # insert status types
    statusList = [(1, 'awaiting_confirmation', 'User has entered email but not confirmed'),
        (2, 'email_confirmed', 'User email has been confirmed'),
        (3, 'awaiting_approval', 'User has registered their information and is waiting for approval'),
        (4, 'approved', 'User has been approved'),
        (5, 'denied', 'User registration was denied')]
    for s in statusList:
        status = UserStatus(user_status_id=s[0], name=s[1], description=s[2])
        userDb.session.add(status)

    # insert user permission types
    typeList = [
        (0, 'agency_user', 'This user is allowed to upload data to be validated'),
        (1, 'website_admin', 'This user is allowed to manage user accounts')]
    for t in typeList:
        type = PermissionType(permission_type_id=t[0],name=t[1], description=t[2])
        userDb.session.add(type)

    userDb.session.commit()
    userDb.session.close()

if __name__ == '__main__':
    setupUserDB(hardReset = True)
