import uuid

from sqlalchemy.sql import func

from dataactcore.models.errorModels import ErrorMetadata
from dataactcore.models.jobModels import Job, Submission
from dataactcore.models.userModel import User, UserStatus
from dataactcore.models.validationModels import RuleSeverity
from dataactcore.interfaces.db import GlobalDB


# First step to deprecating BaseInterface, its children, and corresponding
# interface holders is to start moving all db access logic into one big
# file (to prevent circular imports and have everything in the same place).
# Still to do...make this work solely with a flask context...original idea
# was that these functions would only be invoked within a Flask route, but
# there are some (e.g., createUserWithPassword) that need to be here,
# pending a further refactor.
# As a temporary measure, until the next part of the work that refactors
# the db access within Flask requests, fire up an ad-hoc db session in
# these transitional functions.


# todo: move this value to config when re-factoring location passwords
HASH_ROUNDS = 12


def createUserWithPassword(email, password, bcrypt, permission=1, cgac_code="SYS"):
    """Convenience function to set up fully-baked user (used for setup/testing only)."""
    sess = GlobalDB.db().session
    status = sess.query(UserStatus).filter(UserStatus.name == 'approved').one()
    user = User(email=email, user_status=status, permissions=permission,
                cgac_code=cgac_code, name='Administrator', title='System Admin')
    user.salt, user.password_hash = getPasswordHash(password, bcrypt)
    sess.add(user)
    sess.commit()

    return user


def getPasswordHash(password, bcrypt):
    """Generate password hash."""
    # TODO: handle password hashing/lookup in the User model
    salt = uuid.uuid4().hex
    # number 12 below iw the number of rounds for bcrypt
    hash = bcrypt.generate_password_hash(password + salt, HASH_ROUNDS)
    password_hash = hash.decode("utf-8")
    return salt, password_hash


def populateSubmissionErrorInfo(submissionId):
    """Set number of errors and warnings for submission."""
    sess = GlobalDB.db().session
    submission = sess.query(Submission).filter(Submission.submission_id == submissionId).one()
    submission.number_of_errors = sumNumberOfErrorsForJobList(submissionId)
    submission.number_of_warnings = sumNumberOfErrorsForJobList(submissionId, errorType='warning')
    sess.commit()


def sumNumberOfErrorsForJobList(submissionId, errorType='fatal'):
    """Add number of errors for all jobs in list."""
    sess = GlobalDB.db().session
    errorSum = 0
    jobs = sess.query(Job).filter(Job.submission_id == submissionId).all()
    for job in jobs:
        jobErrors = checkNumberOfErrorsByJobId(job.job_id, errorType)
        if errorType == 'fatal':
            job.number_of_errors = jobErrors
        elif errorType == 'warning':
            job.number_of_warnings = jobErrors
        errorSum += jobErrors
    sess.commit()
    return errorSum


def checkNumberOfErrorsByJobId(jobId, errorType='fatal'):
    """Get the number of errors for a specified job and severity."""
    sess = GlobalDB.db().session
    errors = sess.query(func.sum(ErrorMetadata.occurrences)).\
        join(ErrorMetadata.severity).\
        filter(ErrorMetadata.job_id == jobId, RuleSeverity.name == errorType).scalar()
    # error_metadata table tallies total errors by job/file/field/error type. jobs that
    # don't have errors or warnings won't be in the table at all. thus, if the above query
    # returns an empty value that means the job didn't have any errors that matched
    # the specified severity type, so return 0
    return errors or 0
