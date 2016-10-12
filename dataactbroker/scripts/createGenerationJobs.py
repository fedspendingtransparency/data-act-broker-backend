from dataactcore.interfaces.interfaceHolder import InterfaceHolder
from dataactcore.models.baseInterface import databaseSession
from dataactcore.models.jobModels import Submission, Job, JobDependency

EXTERNAL_FILE_TYPES = ["award_procurement", "award", "awardee_attributes", "sub_award"]
# Get all submission IDs
interfaces = InterfaceHolder()
submissionIds = interfaces.jobDb.session.query(Submission.submission_id).all()
print(str(submissionIds))
fileUpload = interfaces.jobDb.getJobTypeId("file_upload")
validation = interfaces.jobDb.getJobTypeId("csv_record_validation")
ready = interfaces.jobDb.getJobStatusId("ready")
waiting = interfaces.jobDb.getJobStatusId("waiting")
awardTypeId = interfaces.jobDb.getFileTypeId("award")
awardProcTypeId =  interfaces.jobDb.getFileTypeId("award_procurement")
externalIds = []
for fileType in EXTERNAL_FILE_TYPES:
    externalIds.append(interfaces.jobDb.getFileTypeId(fileType))
# For each submission ID, check that all jobs are present and create any missing
print("external IDs: " + str(externalIds))
with databaseSession() as session:
    for submissionId in submissionIds:
        for fileTypeId in externalIds:
            # If job does not exist, create it
            uploadJob = session.query(Job).filter(Job.submission_id == submissionId).filter(Job.file_type_id == fileTypeId).filter(Job.job_type_id == fileUpload).all()
            if uploadJob is None or len(uploadJob) == 0:
                # Create upload job with ready status
                newUploadJob = Job(job_status_id = ready, job_type_id = fileUpload, submission_id = submissionId,
                                   file_type_id = fileTypeId)
                session.add(newUploadJob)
                session.commit()
                uploadId = newUploadJob.job_id
            else:
                uploadId = uploadJob[0].job_id
            # If type is D1 or D2, also create a validation job with waiting status and dependency
            if fileTypeId in [awardTypeId, awardProcTypeId]:
                # Check that validation job exists
                existingValJob = session.query(Job).filter(Job.submission_id == submissionId).filter(Job.file_type_id == fileTypeId).filter(Job.job_type_id == validation).all()
                if existingValJob is None or len(existingValJob) == 0:
                    validationJob = Job(job_status_id = ready, job_type_id = validation, submission_id = submissionId,
                                   file_type_id = fileTypeId)
                    session.add(validationJob)
                    session.commit()
                    dependency = JobDependency(job_id = validationJob.job_id, prerequisite_id = uploadId)
                    session.add(dependency)
                    session.commit()