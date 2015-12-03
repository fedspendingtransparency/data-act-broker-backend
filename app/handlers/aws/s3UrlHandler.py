from datetime import datetime, timedelta
import boto

class s3UrlHandler:
    BASE_URL = "https://s3.amazonaws.com/"
    ENABLE_S3 = False
    URL_LIFETIME = 2000
    def __init__(self,name,user):
        self.bucketRoute = name
        self.user  = user
    def _signUrl(self,path) :
        if(ENABLE_S3) :
            s3connection = boto.connect_s3()
            s3connection.generate_url(URL_LIFETIME, 'PUT', self.bucketRoute, "/"+user+"/" +path)
        return BASE_URL + "/"+self.bucketRoute +"/"+user+"/" +path

    def getSignedUrl(self,fileName,):
        seconds = (datetime.utcnow()-datetime(1970,1,1)).total_seconds()
        return _signUrl(str(seconds)+"_"+fileName)
