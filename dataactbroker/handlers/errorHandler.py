from sqlalchemy.orm import joinedload
from dataactcore.models.errorModels import FileStatus, ErrorData
from dataactcore.models.errorInterface import ErrorInterface

class ErrorHandler(ErrorInterface) :
    """ Manages communication with the error database """

    def getErrorMetricsByJobId (self,jobId) :
        """ Get error metrics for specified job, including number of errors for each field name and error type """
        resultList = []

        query = self.session.query(FileStatus).options(joinedload("status")).filter(FileStatus.job_id == jobId)
        queryResult = self.runUniqueQuery(query,"No file status for this job", "Conflicting file statuses for this job")

        if(not queryResult.status.status_id == self.getStatusId("complete")) :
            return [{"field_name":"File Level Error","error_name": queryResult.status.name,"error_description":str(queryResult.status.description),"occurrences":1,"rule_failed":""}]

        queryResult = self.session.query(ErrorData).options(joinedload("error_type")).filter(ErrorData.job_id == jobId).all()
        for result in queryResult:
            recordDict = {"field_name":result.field_name,"error_name": result.error_type.name, "error_description": result.error_type.description, "occurrences": str(result.occurrences), "rule_failed": result.rule_failed}
            resultList.append(recordDict)
        return resultList
