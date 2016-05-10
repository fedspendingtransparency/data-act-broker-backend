from dataactcore.models.jobModels import JobStatus, JobDependency, Submission
from dataactcore.models.jobTrackerInterface import JobTrackerInterface

def clearJobs():
    """ Clear existing job information from job tracker DB without wiping other DBs.  Should usually be run in conjunction with clearErrors script """
    jobDb = JobTrackerInterface()
    jobDb.session.query(JobDependency).delete()
    jobDb.session.query(JobStatus).delete()
    jobDb.session.query(Submission).delete()
    jobDb.session.commit()
    jobDb.session.close()

if __name__ == '__main__':
    clearJobs()