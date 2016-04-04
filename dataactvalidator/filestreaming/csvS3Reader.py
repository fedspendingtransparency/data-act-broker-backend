import boto
import csv
import os
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.responseException import ResponseException
from dataactvalidator.filestreaming.csvAbstractReader import CsvAbstractReader
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner

class CsvS3Reader(CsvAbstractReader):
    """
    Reads data from S3 CSV file
    """

    def openFile(self,bucket,filename,csvSchema):
        """ Opens file and prepares to read each record, mapping entries to specified column names
        Args:
            bucket : the S3 Bucket
            filename: The file path for the CSV file in S3
        Returns:
        """
        s3connection = boto.connect_s3()
        s3Bucket = s3connection.lookup(bucket)
        self.s3File = s3Bucket.lookup(filename)
        if(self.s3File == None):
            raise ValueError("".join(["Filename provided not found on S3: ",str(filename)]))

        super(CsvS3Reader,self).openFile(bucket,filename,csvSchema)

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
        offsetCheck = self.packetCounter *  CsvAbstractReader.BUFFER_SIZE 
        header ={'Range' : "".join(['bytes=',str(offsetCheck),'-',str(offsetCheck +CsvAbstractReader.BUFFER_SIZE - 1)])}
        try:
            packet = self.s3File.get_contents_as_string(headers=header).decode('utf-8')
            return True, packet
        except :
            return False, ""
