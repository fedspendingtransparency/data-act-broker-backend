import io
import csv
import boto
import smart_open

class CsvWriter(object):
    """
    Writes a CSV to an S3 Bucket in a steaming manner
    use with the "with" python construct
    """

    BUFFER_SIZE =  (5 * 1024 ** 2)
    BATCH_SIZE = 100

    def __init__(self,bucket,filename,header) :
        """

        args

        bucket - the string name of the S3 bucket
        filename - string filename and path in the S3 bucket
        header - list of strings for the header

        """
        self.rows = []
        self.stream = smart_open.smart_open("".join(["s3://",bucket,"/",filename]), 'wb',min_part_size=CsvWriter.BUFFER_SIZE)
        self.write(header)


    @staticmethod
    def doesFileExist(bucket,filename) :
        """

        bucket - the string name of the S3 bucket
        filename - string filename and path in the S3 bucket

        returns true if file can be found
        """
        s3connection = boto.connect_s3()
        s3Bucket = s3connection.lookup(bucket)
        if(s3Bucket is None) :
            return False
        if(s3Bucket.lookup(filename) is None ) :
            return False
        return True

    def write(self,dataList) :
        """

        args
        dataList - list of strings to be written to the S3 bucket
        Adds a row of csv into the S3 stream

        """
        byteList = []
        for data in dataList:
            byteList.append(data.encode("UTF-8"))
        self.rows.append(byteList)
        if(len(self.rows) > self.BATCH_SIZE):
            ioStream = io.BytesIO()
            csvFormatter = csv.writer(ioStream)
            csvFormatter.writerows(self.rows)
            self.stream.write(ioStream.getvalue())
            self.rows = []

    def finishBatch(self):
        """ Write the last unfinished batch """
        ioStream = io.BytesIO()
        csvFormatter = csv.writer(ioStream)
        csvFormatter.writerows(self.rows)
        self.stream.write(ioStream.getvalue())
        self.rows = []

    def __enter__(self) :
        """

        Return a reference of self in the 'with' block

        """
        return self

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
