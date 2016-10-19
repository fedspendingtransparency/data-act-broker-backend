import requests
from dataactcore.config import CONFIG_SERVICES


class ManagerProxy(object):
    """ Temporary bypass of job manager, used to call validator directly """
    MANAGER_FILE  = "manager.json"
    JSON_HEADER = {"Content-Type": "application/json"}

    def jobJson(self,jobId):
        """ Create JSON to hold jobId """
        return '{"job_id":"'+str(jobId)+'"}'

    def sendJobRequest(self,jobId):
        """ Send request to validator """
        validator_host = str(CONFIG_SERVICES['validator_host'])
        validator_port = str(CONFIG_SERVICES['validator_port'])
        if validator_port:
            validator_url = 'http://{}:{}/validate_threaded/'.format(
                validator_host, validator_port)
        else:
            validator_url = 'http://{}/validate_threaded/'.format(
                validator_host)
        return requests.request(method="POST", url=validator_url,
            data=self.jobJson(jobId), headers = self.JSON_HEADER)

