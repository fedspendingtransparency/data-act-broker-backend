from dataactcore.models.errorModels import FileStatus, ErrorData, ErrorType, Status
from dataactcore.models.errorInterface import ErrorInterface
from sqlalchemy.orm import subqueryload, joinedload
class ErrorHandler(ErrorInterface) :

    def getErrorMetericsByJobId (self,jobId) :
        resultList = []
        queryResult = self.session.query(ErrorData).options(joinedload("error_type")).filter(ErrorData.job_id == jobId).all()
        for result in queryResult:
            if(result.error_type is None) :
                errorType  = result.rule_failed
            else :
                errorType  = result.error_type.description
            recordList = [result.field_name,str(result.occurrences),errorType]
            resultList.append(recordList)
        return resultList
