from dataactcore.models.errorModels import File, ErrorMetadata
from dataactcore.models.errorInterface import ErrorInterface


def clearErrors():
    """Clear all error-related data from database."""
    errorDb = ErrorInterface()
    errorDb.session.query(ErrorMetadata).delete()
    errorDb.session.query(File).delete()
    errorDb.session.commit()
    errorDb.close()


def clearErrorsByJobId(jobId):
    """Clear all errors for specified job."""
    errorDb = ErrorInterface()
    errorDb.session.query(ErrorMetadata).filter(job_id == jobId).delete()
    errorDb.session.query(File).filter(job_id == jobId).delete()
    errorDb.session.commit()
    errorDb.close()


if __name__ == '__main__':
    clearErrors()
