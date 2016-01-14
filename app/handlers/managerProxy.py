import requests
import json
import os
import inspect

class ManagerProxy(object):
    MANAGER_FILE  = "manager.json"
    JSON_HEADER = {"Content-Type": "application/json"}

    def _getPath(self,):
        path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        lastBackSlash = path.rfind("\\",0,-1)
        lastForwardSlash = path.rfind("/",0,-1)
        lastSlash = max([lastBackSlash,lastForwardSlash])
        credFile = path[0:lastSlash] + "/" + ManagerProxy.MANAGER_FILE
        return json.loads(open(credFile,"r").read())["URL"]

    def jobJson(self,jobId):
        """ Create JSON to hold jobId """
        return '{"job_id":"'+str(jobId)+'"}'

    def sendJobRequest(self,jobId):
        return requests.request(method="POST", url=self._getPath() + "/validate_threaded/", data=self.jobJson(jobId), headers = self.JSON_HEADER)

