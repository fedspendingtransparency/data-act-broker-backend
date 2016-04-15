from dataactcore.models.errorModels import FileStatus, ErrorData
from dataactcore.models.errorInterface import ErrorInterface


def clearErrors():
    """Clear all error-related data from database."""
    errorDb = ErrorInterface()
    errorDb.session.query(ErrorData).delete()
    errorDb.session.query(FileStatus).delete()
    errorDb.session.commit()
    errorDb.session.close()


def clearErrorsByJobId(jobId):
    """Clear all errors for specified job."""
    errorDb = ErrorInterface()
    errorDb.session.query(ErrorData).filter(job_id==jobId).delete()
    errorDb.session.query(FileStatus).filter(job_id==jobId).delete()
    errorDb.session.commit()
    errorDb.session.close()


if __name__ == '__main__':
    clearErrors()
