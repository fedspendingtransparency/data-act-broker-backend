import boto
from dataactvalidator.filestreaming.csvAbstractReader import CsvAbstractReader


class CsvS3Reader(CsvAbstractReader):
    """
    Reads data from S3 CSV file
    """

    def openFile(self, region, bucket, filename, csvSchema, bucketName, errorFilename):
        """ Opens file and prepares to read each record, mapping entries to specified column names
        Args:
            bucket : the S3 Bucket
            filename: The file path for the CSV file in S3
        Returns:
        """
        s3connection = boto.s3.connect_to_region(region)
        s3Bucket = s3connection.lookup(bucket)
        self.s3File = s3Bucket.lookup(filename)
        self.isLocal = False
        if not self.s3File:
            raise ValueError("Filename {} not found on S3".format(filename))

        super(CsvS3Reader, self).openFile(
            region, bucket, filename, csvSchema, bucketName, errorFilename)

    def downloadFile(self, region, bucket, filename, targetLocation):
        """ After opening a file, download entire file to filename """
        s3connection = boto.s3.connect_to_region(region)
        s3Bucket = s3connection.lookup(bucket)
        if not s3Bucket:
            raise ValueError("Bucket {} not found in region {}".format(
                bucket, region))
        self.s3File = s3Bucket.get_key(filename)
        if not self.s3File:
            raise ValueError("Filename {} not found in bucket {}".format(
                filename, bucket))
        self.s3File.get_contents_to_filename(targetLocation)

    def close(self):
        """ Don't need to close file when streaming from S3 """
        pass

    def _getFileSize(self):
        """
        Gets the size of the file
        """
        return self.s3File.size

    def _getNextPacket(self):
        """
        Gets the next packet from the file returns true if successful
        """
        offsetCheck = self.packetCounter * CsvAbstractReader.BUFFER_SIZE
        header = {'Range': 'bytes={}-{}'.format(
            offsetCheck, offsetCheck + CsvAbstractReader.BUFFER_SIZE - 1)}
        try:
            packet = self.s3File.get_contents_as_string(headers=header)
            return True, packet
        except:
            return False, ""
