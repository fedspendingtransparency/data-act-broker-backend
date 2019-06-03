import boto3
import requests
import tempfile
import urllib
from dataactcore.config import CONFIG_BROKER

VALID_SCHEMES = ("http", "https", "s3", "file", "")
SCHEMA_HELP_TEXT = (
    "Internet RFC on Relative Uniform Resource Locators " +
    "Format: scheme://netloc/path;parameters?query#fragment " +
    "List of supported schemes: " +
    ", ".join(["{}://".format(s) for s in VALID_SCHEMES if s])
)


class RetrieveFileFromUri:
    def __init__(self, ruri, binary_data=True):
        """ Class to return a temporary file object representing the file specified by the URI

            Attributes:
                uri: the URI to process
                mode: way the file object reads in the data
        """
        self.uri = ruri  # Relative Uniform Resource Locator
        self.mode = "rb" if binary_data else "r"
        self._validate_url()

    def _validate_url(self):
        """ Parses and validates the URI provided """

        self.parsed_url_obj = urllib.parse.urlparse(self.uri)
        self._test_approved_scheme()

    def _test_approved_scheme(self):
        """ Validate the URI scheme provided

            Raises:
                NotImplementedError: When the schema provided is not supported
        """

        if self.parsed_url_obj.scheme not in VALID_SCHEMES:
            msg = "Scheme '{}' isn't supported. Try one of these: {}"
            raise NotImplementedError(msg.format(self.parsed_url_obj.scheme, VALID_SCHEMES))

    def get_file_object(self):
        """ Provide a file object (aka file handler) representing the URI provided.
            Note that this simply opens the file so recommendation is to use this method as a context manager

            Returns:
                file object/handler representing the URI provided

            Raises:
                NotImplementedError: When the schema provided isn't supported by this class
        """
        if self.parsed_url_obj.scheme == "s3":
            return self._handle_s3()
        elif self.parsed_url_obj.scheme.startswith("http"):
            return self._handle_http()
        elif self.parsed_url_obj.scheme in ("file", ""):
            return self._handle_file()
        else:
            raise NotImplementedError("No handler for scheme: {}!".format(self.parsed_url_obj.scheme))

    def _handle_s3(self):
        """ Handler for S3 URIs

            Returns:
                temporary file object pulled from S3
        """
        # remove leading '/' character
        file_path = self.parsed_url_obj.path[1:]
        boto3_s3 = boto3.resource("s3", region_name=CONFIG_BROKER['aws_region'])
        s3_bucket = boto3_s3.Bucket(self.parsed_url_obj.netloc)

        # Must be in binary mode (default)
        f = tempfile.SpooledTemporaryFile()
        s3_bucket.download_fileobj(file_path, f)
        if self.mode == "r":
            byte_str = f._file.getvalue()
            f = tempfile.SpooledTemporaryFile(mode=self.mode)
            f.write(byte_str.decode())
        # go to beginning of file for reading
        f.seek(0)
        return f

    def _handle_http(self):
        """ Handler for HTTP URIs

            Returns:
                temporary file object pulled from HTTP
        """
        r = requests.get(self.uri, allow_redirects=True)
        f = tempfile.SpooledTemporaryFile()
        f.write(r.content)
        # go to beginning of file for reading
        f.seek(0)
        return f

    def _handle_file(self):
        """ File handler for file URIs

            Returns:
                file object from file URI
        """
        if self.parsed_url_obj == "file":
            file_path = self.parsed_url_obj.netloc
        else:
            # if no schema provided, treat it as a relative file path
            file_path = self.parsed_url_obj.path

        return open(file_path, self.mode)
