from dataactcore.models.jobModels import Submission
from dataactcore.models.userModel import User
from dataactcore.models.validationInterface import ValidationInterface
from dataactcore.models.userInterface import UserInterface
from dataactcore.models.jobTrackerInterface import JobTrackerInterface
from dataactcore.utils.responseException import ResponseException

def updateCGAC():
    """
    Create job tracker tables from model metadata.

    NOTE: THIS CAN ONLY BE USED IN SPECIFIC SITUATIONS WHERE THE AGENCY NAMES CURRENTLY
    IN THE FIELD MATCH THE CGAC AGENCY NAME EXACTLY
    """
    jobDb = JobTrackerInterface()
    userDb = UserInterface()
    validationDb = ValidationInterface()


    all_cgac = validationDb.getAllAgencies()
    all_users = userDb.session.query(User).all()
    all_submissions = jobDb.session.query(Submission).all()

    users_to_update = []

    for user in all_users:
        if user.cgac_code != None:
            users_to_update.append(user)

    for user in users_to_update:
        if user.user_id == 2:
            if validAgency(user.cgac_code, validationDb):
                user.cgac_code = validationDb.getCGACCode(user.cgac_code)
            if validCGAC(user.cgac_code, validationDb):
                user_subs = jobDb.getSubmissionsByUserId(user.user_id)
                if user_subs is not None:
                    for sub in user_subs:
                        sub.cgac_code = user.cgac_code

    jobDb.session.commit()
    jobDb.session.close()

    userDb.session.commit()
    userDb.session.close()

    validationDb.session.close()

def validCGAC(cgac_code, validationDB):
    try:
        validationDB.getAgencyName(cgac_code)
        return True
    except ResponseException:
        return False

def validAgency(agency_name, validationDB):
    try:
        validationDB.getCGACCode(agency_name)
        return True
    except ResponseException:
        return False

def findCGACCode(all_cgac, agency_name):
    for cgac in all_cgac:
        if cgac.agency_name == agency_name:
            return cgac.cgac_code
    raise ValueError("CRITICAL ERROR: CGAC CODE NOT FOUND!")


if __name__ == '__main__':
    updateCGAC()
