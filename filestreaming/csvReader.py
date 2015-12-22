import boto
import csv
import re
from dataactcore.utils.responseException import ResponseException

class CsvReader(object):
    """
    Reads data from S3 CSV file
    """

    BUFFER_SIZE = 8192

    def openFile(self,bucket,filename,csvSchema):
        """ Opens file and prepares to read each record, mapping entries to specified column names
        Args:
            bucket : the S3 Bucket
            filename: The file path for the CSV file in S3
        Returns:
        """
        s3connection = boto.connect_s3()
        s3Bucket = s3connection.lookup(bucket)

        possibleFields = {}
        currentFields = {}
        for schema in  csvSchema:
                possibleFields[schema.name] = 0

        self.s3File = s3Bucket.lookup(filename)
        self.unprocessed = ''
        self.extraLine = False
        self.lines = []
        self.headerDictionary = {}
        self.packetCounter = 0;
        current = 0
        self.isFinished = False
        self.columnCount = 0
        line = self._getLine()
        # make sure we have not finished reading the file

        if(self.isFinished) :
             raise ResponseException("CSV file must have a header",400,ValueError)

        #create the header
        for row in csv.reader([line],dialect='excel'):
            for cell in row :
                headerValue = cell.strip().lower()
                if( not headerValue in possibleFields) :
                    raise ResponseException(("Header : "+ headerValue + " not in CSV schema"), 400, ValueError)
                if(possibleFields[headerValue] == 1) :
                    raise ResponseException(("Header : "+ headerValue + " is duplicated"), 400, ValueError)
                self.headerDictionary[(current)] = headerValue
                possibleFields[headerValue]  = 1
                current += 1
        self.columnCount = current
        #Check that all required fields exists
        for schema in csvSchema :
            if(schema.required and  possibleFields[schema.name] == 0) :
                raise ResponseException(("Header : "+ schema.name + " is required"), 400, ValueError)

    def getNextRecord(self):
        """
        Read the next record into a dict and return it
        Returns:
            dictionary representing this record
        """
        current = 0
        returnDict = {}
        line = self._getLine()

        for row in csv.reader([line],dialect='excel'):
            for cell in row :
                if(current >= self.columnCount) :
                    raise ResponseException("Record contains too many fields",ValueError)
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
        #for packet in self.s3File :
        while( self.packetCounter *  CsvReader.BUFFER_SIZE <=  self.s3File.size) :
            offsetCheck = self.packetCounter *  CsvReader.BUFFER_SIZE
            header ={'Range' : 'bytes='+str(offsetCheck)+'-'+str(offsetCheck +CsvReader.BUFFER_SIZE) }
            try:
                packet = self.s3File.get_contents_as_string(headers=header)
            except :
                # Exit
                break
            self.packetCounter +=1

            #Get the current lines
            currentBytes = self.unprocessed + packet
            self.lines = self._splitLines(currentBytes)

            #edge case if the packet was filled with newlines only try again
            if( len(self.lines) ==0 ):
                continue

            #last line still needs processing save and reuse
            self.unprocessed = self.lines.pop()
            if(len(self.lines) > 0) :
                #Get the next line
                return  self.lines.pop(0)
        self.isFinished = True
        if(len(self.unprocessed) < 2):
            # Got an extra line from a line break on the last line
            self.extraLine = True
        return self.unprocessed

    def _splitLines(self,packet) :
        """
        arguments :
        packet unprocessed string of CSV data
        returns a list of strings broken by newline
        """
        linesToReturn = []
        ecapeMode =  False
        current = ""

        index = 0
        for  char in packet :
            if(not ecapeMode) :
                if(char =='\r' or char =='\n' or char =='\r\n') :
                    if (len(current) >0 ) :
                        linesToReturn.append(current)
                        #check the last char if its a new line add extra line
                        # as its at the end of the packet
                        if( index == len(packet)-1 ) :
                            linesToReturn.append("")
                    current = ""
                else :
                  current = current + char
                  if(char == '"') :
                        ecapeMode = True
            else :
                if(char == '"') :
                    ecapeMode = False
                current = current + char
            index+=1
        if (len(current)>0) :
            linesToReturn.append(current)
        return linesToReturn
