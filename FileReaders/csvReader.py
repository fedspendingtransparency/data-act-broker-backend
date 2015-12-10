import boto
import csv
import re

class CsvReader(object):
    """
    Reads data from S3 CSV file
    """
    def openFile(self,bucket,filename):
        """ Opens file and prepares to read each record, mapping entries to specified column names
        Args:
            bucket : the S3 Bucket
            filename: The file path for the CSV file in S3
        Returns:
        """
        s3connection = boto.connect_s3()
        s3Bucket = s3connection.lookup(bucket)
        self.s3File = s3Bucket.lookup(filename)
        self.unprocessed = ''
        self.lines = []
        self.headerDictionary = {}
        current = 0
        self.isFinished = False
        self.columnCount = 0
        line = self._getLine()
        # make sure we have not finished reading the file
        if(self.isFinished) :
             raise ValueError("CSV file must have a header")

        #create the header
        for row in csv.reader([line],dialect='excel'):
            for cell in row :
                self.headerDictionary[(current)] = cell
                current += 1
        self.columnCount = current
    def getNextRecord(self):
        """
        Read the next record into a dict and return it
        Returns:
            dictionary representing this record
        """
        current = 0
        returnDict = {}
        line = self._getLine()
        if(not self.isFinished):
            for row in csv.reader([line],dialect='excel'):
                for cell in row :
                    if(current >= self.columnCount) :
                        raise ValueError("Record contains to many fields")
                    returnDict[self.headerDictionary[current]] = cell
                    current += 1

        return returnDict

    def _getLine(self):
        """
        This method reads 8192 bytes from S3 Bucket at a time and stores
        it in a line buffer. The line buffer is used untill its empty then
        another request is created to S3 for more data.
        """
        if(len(self.lines) > 0) :
            #Get the next line
            return self.lines.pop(0)
        #packets are 8192 bytes in size
        for packet in self.s3File :
            currentBytes = self.unprocessed + packet
            self.lines = re.split(r'[\n\r]+', currentBytes)
            #last line still needs processing save and reuse
            self.unprocessed = self.lines.pop()
            if(len(self.lines) > 0) :
                #Get the next line
                return  self.lines.pop(0)

        self.isFinished = True
