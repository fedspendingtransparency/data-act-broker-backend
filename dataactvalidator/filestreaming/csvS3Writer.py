import io
import csv
import boto
import smart_open
from dataactvalidator.filestreaming.csvAbstractWriter import CsvAbstractWriter

class CsvS3Writer(CsvAbstractWriter):
    """
    Writes a CSV to an S3 Bucket in a steaming manner
    use with the "with" python construct
    """
    def __init__(self,region,bucket,filename,header) :
        """

        args

        bucket - the string name of the S3 bucket
        filename - string filename and path in the S3 bucket
        header - list of strings for the header

        """
        conn = boto.s3.connect_to_region(region).get_bucket(bucket).new_key(filename)
        self.stream = smart_open.smart_open(conn, 'w', min_part_size=CsvAbstractWriter.BUFFER_SIZE)
        super(CsvS3Writer,self).__init__(filename,header)


    def _write(self,data):
        """

        args

        data -  (string) a string be written to the current file

        """
        self.stream.write(data)


    def __exit__(self, type, value, traceback) :
        """

        args
        type - the type of error
        value - the value of the error
        traceback - the traceback of the error

        This function calls the smart open exit in the
        'with' block

        """
        self.stream.__exit__(type, value, traceback)
