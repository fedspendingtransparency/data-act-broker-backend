import uuid

from sqlalchemy.sql import func

from dataactcore.models.errorModels import ErrorMetadata, File
from dataactcore.models.jobModels import Job, Submission, JobDependency
from dataactcore.models.userModel import User, UserStatus, PermissionType
from dataactcore.models.validationModels import RuleSeverity
from dataactcore.models.lookups import FILE_TYPE_DICT, JOB_TYPE_DICT, JOB_STATUS_DICT
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


# todo: move these value to config when re-factoring user management
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


def getUsersByType(permissionName):
    """Get list of users with specified permission."""
    sess = GlobalDB.db().session
    # This could likely be simplified, but since we're moving towards using MAX for authentication,
    # it's not worth spending too much time reworking.
    userList = []
    bitNumber = sess.query(PermissionType).filter(PermissionType.name == permissionName).one().permission_type_id
    users = sess.query(User).all()
    for user in users:
        if checkPermissionByBitNumber(user, bitNumber):
            # This user has this permission, include them in list
            userList.append(user)
    return userList


def checkPermissionByBitNumber(user, bitNumber):
    """Check whether user has the specified permission, determined by whether a binary representation of user's
    permissions has the specified bit set to 1.  Use hasPermission to check by permission name."""
    # This could likely be simplified, but since we're moving towards using MAX for authentication,
    # it's not worth spending too much time reworking.

    if user.permissions == None:
        # This user has no permissions
        return False
    # First get the value corresponding to the specified bit (i.e. 2^bitNumber)
    bitValue = 2 ** (bitNumber)
    # Remove all bits above the target bit by modding with the value of the next higher bit
    # This leaves the target bit and all lower bits as the remaining value, all higher bits are set to 0
    lowEnd = user.permissions % (bitValue * 2)
    # Now compare the remaining value to the value for the target bit to determine if that bit is 0 or 1
    # If the remaining value is still at least the value of the target bit, that bit is 1, so we have that permission
    return lowEnd >= bitValue


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


def addJobsForFileType(fileType, filePath, filename, submissionId, existingSubmission, jobsRequired, uploadDict):
    """ Add upload and validation jobs for a single filetype

    Args:
        fileType: What type of file to add jobs for
        filePath: Path to upload the file to
        filename: Original filename
        submissionId -- Submission ID to attach to jobs
        existingSubmission -- True if we should update existing jobs rather than creating new ones
        jobsRequired: List of job ids that will be prerequisites for cross-file job
        uploadDict: Dictionary of upload ids by filename to return to client, used for calling finalize_submission route

    Returns:
        jobsRequired: List of job ids that will be prerequisites for cross-file job
        uploadDict: Dictionary of upload ids by filename to return to client, used for calling finalize_submission route
    """
    sess = GlobalDB.db().session

    fileTypeId = FILE_TYPE_DICT[fileType]

    # Create a file upload job or, for an existing submission, modify the
    # existing upload job.

    if existingSubmission:
        # mark existing upload job as running
        uploadJob = sess.query(Job).filter_by(
            submission_id=submissionId,
            file_type_id=fileTypeId,
            job_type_id=JOB_TYPE_DICT['file_upload']
        ).one()
        # Mark as running and set new file name and path
        uploadJob.job_status_id = JOB_STATUS_DICT['running']
        uploadJob.original_filename = filename
        uploadJob.filename = filePath

    else:
        if fileType in ["award", "award_procurement"]:
            # file generation handled on backend, mark as ready
            uploadStatus = JOB_STATUS_DICT['ready']
        elif fileType in ["awardee_attributes", "sub_award"]:
            # these are dependent on file D2 validation
            uploadStatus = JOB_STATUS_DICT['waiting']
        else:
            # mark as running since frontend should be doing this upload
            uploadStatus = JOB_STATUS_DICT['running']

        uploadJob = Job(original_filename=filename, filename=filePath, file_type_id=fileTypeId,
                        job_status_id=uploadStatus, job_type_id=JOB_TYPE_DICT['file_upload'],
                        submission_id=submissionId)
        sess.add(uploadJob)

    sess.flush()

    # Create a file validation job or, for an existing submission, modify the
    # existing validation job.

    if existingSubmission:
        # if the file's validation job is attached to an existing submission,
        # reset its status and delete any validation artifacts (e.g., error metadata) that
        # might exist from a previous run.
        valJob = sess.query(Job).filter_by(
            submission_id=submissionId,
            file_type_id=fileTypeId,
            job_type_id=JOB_TYPE_DICT['csv_record_validation']
        ).one()
        valJob.job_status_id = JOB_STATUS_DICT['waiting']
        valJob.original_filename = filename
        valJob.filename = filePath
        # Reset file size and number of rows to be set during validation of new file
        valJob.file_size = None
        valJob.number_of_rows = None
        # Delete error metdata this might exist from a previous run of this validation job
        sess.query(ErrorMetadata).\
            filter(ErrorMetadata.job_id == valJob.job_id).\
            delete(synchronize_session='fetch')
        # Delete file error information that might exist from a previous run of this validation job
        sess.query(File).filter(File.job_id == valJob.job_id).delete(synchronize_session='fetch')

    else:
        # create a new record validation job and add dependencies if necessary
        if fileType == "awardee_attributes":
            d1ValJob = sess.query(Job).\
                filter(Job.submission_id == submissionId,
                       Job.file_type_id == FILE_TYPE_DICT['award_procurement'],
                       Job.job_type_id == JOB_TYPE_DICT['csv_record_validation']).\
                first()
            if d1ValJob is None:
                raise Exception("Cannot create E job without a D1 job")
            # Add dependency on D1 validation job
            d1Dependency = JobDependency(job_id=uploadJob.job_id, prerequisite_id=d1ValJob.job_id)
            sess.add(d1Dependency)

        elif fileType == "sub_award":
            # todo: check for C validation job
            cValJob = sess.query(Job). \
                filter(Job.submission_id == submissionId,
                       Job.file_type_id == FILE_TYPE_DICT['award_financial'],
                       Job.job_type_id == JOB_TYPE_DICT['csv_record_validation']). \
                first()
            if cValJob is None:
                raise Exception("Cannot create F job without a C job")
            # Add dependency on C validation job
            cDependency = JobDependency(job_id=uploadJob.job_id, prerequisite_id=cValJob.job_id)
            sess.add(cDependency)

        else:
            # E and F don't get validation jobs
            valJob = Job(original_filename=filename, filename=filePath, file_type_id=fileTypeId,
                         job_status_id=JOB_STATUS_DICT['waiting'],
                         job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                         submission_id=submissionId)
            sess.add(valJob)
            sess.flush()
            # Add dependency between file upload and db upload
            uploadDependency = JobDependency(job_id=valJob.job_id, prerequisite_id=uploadJob.job_id)
            sess.add(uploadDependency)
            jobsRequired.append(valJob.job_id)

    sess.commit()

    uploadDict[fileType] = uploadJob.job_id
    return jobsRequired, uploadDict
