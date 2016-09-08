from dataactcore.models.jobModels import Job, JobDependency, Submission
from dataactcore.models.jobTrackerInterface import JobTrackerInterface

def clearJobs():
    """ Clear existing job information from job tracker DB without wiping other DBs.  Should usually be run in conjunction with clearErrors script """
    jobDb = JobTrackerInterface()
    jobDb.session.query(JobDependency).delete()
    jobDb.session.query(Job).delete()
    jobDb.session.query(Submission).delete()
    jobDb.session.commit()
    jobDb.close()

if __name__ == '__main__':
    clearJobs()
