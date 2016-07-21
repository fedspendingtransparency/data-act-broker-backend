import io
import csv
import boto
import smart_open
from dataactvalidator.filestreaming.csvAbstractWriter import CsvAbstractWriter
class CsvLocalWriter(CsvAbstractWriter):
    """
    Writes a CSV to the local file system in a steaming manner
    use with the "with" python construct
    """

    def __init__(self,filename,header) :
        """

        args

        bucket - the string name of the S3 bucket
        filename - string filename and path in the S3 bucket
        header - list of strings for the header

        """
        self.stream = open(filename,"w")
        super(CsvLocalWriter,self).__init__(filename,header)


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
        self.stream.close()
