import boto
from dataactvalidator.filestreaming.csvAbstractReader import CsvAbstractReader


class CsvS3Reader(CsvAbstractReader):
    """
    Reads data from S3 CSV file
    """

    def initializeFile(self, region, bucket, filename):
        """Returns an S3 filename."""
        s3connection = boto.s3.connect_to_region(region)
        s3Bucket = s3connection.lookup(bucket)
        if not s3Bucket:
            raise ValueError("Bucket {} not found in region {}".format(
                bucket, region))
        s3File = s3Bucket.get_key(filename)
        if not s3File:
            raise ValueError("Filename {} not found in bucket {}".format(
                filename, bucket))
        return s3File


    def openFile(self, region, bucket, filename, csvSchema, bucketName, errorFilename):
        """ Opens file and prepares to read each record, mapping entries to specified column names
        Args:
            bucket : the S3 Bucket
            filename: The file path for the CSV file in S3
        Returns:
        """
        self.s3File = self.initializeFile(region, bucket, filename)
        self.isLocal = False

        super(CsvS3Reader, self).openFile(
            region, bucket, filename, csvSchema, bucketName, errorFilename)

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
            packet = self.s3File.get_contents_as_string(headers=header, encoding='utf-8')
            return True, packet
        except:
            return False, ""
