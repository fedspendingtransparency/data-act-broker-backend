import boto
import csv
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.responseException import ResponseException
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner

class CsvAbstractReader(object):
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


        possibleFields = {}
        currentFields = {}
        for schema in  csvSchema:
                possibleFields[FieldCleaner.cleanString(schema.name)] = 0

        self.filename = filename
        self.unprocessed = ''
        self.extraLine = False
        self.lines = []
        self.headerDictionary = {}
        self.packetCounter = 0
        current = 0
        self.isFinished = False
        self.columnCount = 0
        line = self._getLine()
        # make sure we have not finished reading the file

        if(self.isFinished) :
            raise ResponseException("CSV file must have a header",StatusCode.CLIENT_ERROR,ValueError,ValidationError.singleRow)

        #create the header
        for row in csv.reader([line],dialect='excel'):
            for cell in row :
                headerValue = FieldCleaner.cleanString(cell)
                if( not headerValue in possibleFields) :
                    # Allow unexpected headers, just mark the header as None so we skip it when reading
                    self.headerDictionary[(current)] = None
                    current += 1
                elif(possibleFields[headerValue] == 1) :
                    raise ResponseException(("".join(["Header : ",headerValue," is duplicated"])), StatusCode.CLIENT_ERROR, ValueError,ValidationError.duplicateError)
                else:
                    self.headerDictionary[(current)] = headerValue
                    possibleFields[headerValue]  = 1
                    current += 1
        self.columnCount = current
        #Check that all required fields exists
        missingHeaders = []
        for schema in csvSchema :
            if(schema.required and  possibleFields[FieldCleaner.cleanString(schema.name)] == 0) :
                missingHeaders.append(schema.name)
        if(len(missingHeaders) > 0):
            # Add missing headers to exception, to be stored in file_status table
            missingHeaderString = ", ".join(missingHeaders)
            raise ResponseException(("".join(["Missing required headers: ", missingHeaderString])), StatusCode.CLIENT_ERROR, ValueError,ValidationError.missingHeaderError,extraInfo=missingHeaderString)

    def getNextRecord(self):
        """
        Read the next record into a dict and return it
        Returns:
            dictionary representing this record
        """
        returnDict = {}
        line = self._getLine()

        for row in csv.reader([line],dialect='excel'):
            for current, cell in enumerate(row):
                if(current >= self.columnCount) :
                    raise ResponseException("Record contains too many fields",StatusCode.CLIENT_ERROR,ValueError,ValidationError.readError)
                if(cell == ""):
                    # Use None instead of empty strings for sqlalchemy
                    cell = None
                if self.headerDictionary[current] is None:
                    # Skip this column as it is unknown
                    continue
                else:
                    returnDict[self.headerDictionary[current]] = cell
        return returnDict

    def close(self):
        """
        closes the file
        """
        pass

    def _getFileSize(self):
        """
        Gets the size of the file
        """
        return 0

    def _getNextPacket(self):
        """
        Gets the next packet from the file returns true if successful
        """
        return False , ""

    def _getLine(self):

        """
        This method reads 8192 bytes from S3 Bucket at a time and stores
        it in a line buffer. The line buffer is used until its empty then
        another request is created to S3 for more data.
        """
        if(len(self.lines) > 0) :
            #Get the next line
            return self.lines.pop(0)
        #packets are 8192 bytes in size
        #for packet in self.s3File :
        while( self.packetCounter *  CsvAbstractReader.BUFFER_SIZE <=  self._getFileSize()) :

            success,packet =  self._getNextPacket()
            if(not success) :
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
        if(len(self.unprocessed) < 5):
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

        for  index,char in enumerate(packet):
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
                  current = "".join([current,char])
                  if(char == '"') :
                        ecapeMode = True
            else :
                if(char == '"') :
                    ecapeMode = False
                current = "".join([current,char]) #current.join([char])
        if (len(current)>0) :
            linesToReturn.append(current)
        return linesToReturn
