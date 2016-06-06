from dataactcore.models.jobModels import Submission
from dataactcore.models.userModel import User
from dataactcore.models.validationInterface import ValidationInterface
from dataactcore.models.userInterface import UserInterface
from dataactcore.models.jobTrackerInterface import JobTrackerInterface

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

    for user in all_users:
        user.cgac_code = findCGACCode(all_cgac, user.cgac_code)

    for submission in all_submissions:
        submission.cgac_code = findCGACCode(all_cgac, submission.cgac_code)

    jobDb.session.commit()
    jobDb.session.close()

    userDb.session.commit()
    userDb.session.close()

    # validationDb.session.commit()
    validationDb.session.close()

def findCGACCode(all_cgac, agency_name):
    for cgac in all_cgac:
        if cgac.agency_name == agency_name:
            return cgac.cgac_code
    raise ValueError("CRITICAL ERROR: CGAC CODE NOT FOUND!")


if __name__ == '__main__':
    updateCGAC()
