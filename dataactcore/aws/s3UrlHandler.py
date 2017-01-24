import logging
from datetime import datetime
import boto
from boto import sts
from dataactcore.config import CONFIG_BROKER


logger = logging.getLogger(__name__)


class S3UrlHandler:
    """
    This class acts a wrapper for S3 URL Signing
    """
    BASE_URL = "https://s3.amazonaws.com/"
    ENABLE_S3 = True
    URL_LIFETIME = 2000
    STS_LIFETIME = 2000
    S3_ROLE = ""

    def __init__(self, name=None):
        """
        Creates the object for signing URLS

        arguments:
        name -- (String) Name of the S3 bucket

        """
        if name is None:
            self.bucketRoute = CONFIG_BROKER['aws_bucket']
        else:
            self.bucketRoute = name

        S3UrlHandler.S3_ROLE = CONFIG_BROKER['aws_role']
        S3UrlHandler.REGION = CONFIG_BROKER['aws_region']

    def _sign_url(self, path, file_name, bucket_route, method="PUT"):
        """
        Creates the object for signing URLS

        arguments:
            path - (String) path to folder
            fileName - (String) File name of file to be uploaded to S3.
            method - method to create signed url for
        returns signed url (String)

        """
        if S3UrlHandler.ENABLE_S3:
            s3connection = boto.s3.connect_to_region(S3UrlHandler.REGION)
            if method == "PUT":
                return s3connection.generate_url(S3UrlHandler.URL_LIFETIME, method,
                                                 bucket_route, "/" + path + "/" + file_name,
                                                 headers={'Content-Type': 'application/octet-stream'})
            return s3connection.generate_url(S3UrlHandler.URL_LIFETIME, method,
                                             bucket_route, "/" + path + "/" + file_name)
        return S3UrlHandler.BASE_URL + "/" + self.bucketRoute + "/" + path + "/" + file_name

    def get_signed_url(self, path, file_name, bucket_route=None, method="PUT"):
        """
        Signs a URL for PUT requests

        arguments:
        file_name -- (String) File name of file to be uploaded to S3.

        returns signed url (String)
        """
        bucket_route = self.bucketRoute if bucket_route is None else bucket_route

        if method == "PUT":
            self.s3FileName = S3UrlHandler.get_timestamped_filename(file_name)
        else:
            self.s3FileName = file_name
        return self._sign_url(path, self.s3FileName, bucket_route, method)

    @staticmethod
    def get_timestamped_filename(filename):
        """
        Gets a Timestamped file name to prevent conflicts on S3 Uploading
        """
        seconds = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds())
        return str(seconds) + "_" + filename

    @staticmethod
    def get_temporary_credentials(user):
        """
        Gets token that allows for S3 Uploads for seconds set in STS_LIFETIME
        """
        sts_connection = sts.connect_to_region(S3UrlHandler.REGION)
        role = sts_connection.assume_role(S3UrlHandler.S3_ROLE, "FileUpload" + str(user),
                                          duration_seconds=S3UrlHandler.STS_LIFETIME)
        credentials = {
            'AccessKeyId': role.credentials.access_key,
            'SecretAccessKey': role.credentials.secret_key,
            'SessionToken': role.credentials.session_token,
            'Expiration': role.credentials.expiration
        }
        return credentials

    @staticmethod
    def get_file_size(filename):
        """ Returns file size in number of bytes for specified filename, or False if file doesn't exist """

        # Get key
        try:
            S3UrlHandler.REGION
        except AttributeError:
            S3UrlHandler.REGION = CONFIG_BROKER["aws_region"]
        s3connection = boto.s3.connect_to_region(S3UrlHandler.REGION)
        bucket = s3connection.get_bucket(CONFIG_BROKER['aws_bucket'])
        key = bucket.get_key(filename)
        if key is None:
            logger.warning("File doesn't exist on AWS: %s", filename)
            return 0
        else:
            return key.size

    def get_file_urls(self, bucket_name, path):
        try:
            S3UrlHandler.REGION
        except AttributeError:
            S3UrlHandler.REGION = CONFIG_BROKER["aws_region"]

        s3connection = boto.s3.connect_to_region(S3UrlHandler.REGION)
        bucket = s3connection.get_bucket(bucket_name)

        urls = {}

        for key in bucket.list(prefix=path):
            if key.name != path:
                file_name = key.name[len(path):]
                url = self.get_signed_url(path=path, file_name=file_name, bucket_route=bucket_name, method="GET")
                urls[file_name] = url

        return urls
