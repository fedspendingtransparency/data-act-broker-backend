from datetime import datetime, timedelta
import boto

class s3UrlHandler:
    BASE_URL = "https://s3.amazonaws.com/"
    ENABLE_S3 = True
    URL_LIFETIME = 2000
    def __init__(self,name,user):
        self.bucketRoute = name
        self.user  = user
    def _signUrl(self,path) :
        if(s3UrlHandler.ENABLE_S3) :
            s3connection = boto.connect_s3()
            return s3connection.generate_url(s3UrlHandler.URL_LIFETIME, 'PUT', self.bucketRoute, "/"+self.user+"/" +path)
        return s3UrlHandler.BASE_URL + "/"+self.bucketRoute +"/"+self.user+"/" +path

    def getSignedUrl(self,fileName,):
        seconds = int((datetime.utcnow()-datetime(1970,1,1)).total_seconds())
        return self._signUrl(fileName)
