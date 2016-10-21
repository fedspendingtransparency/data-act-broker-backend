import csv
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.validationInterface import ValidationInterface
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.responseException import ResponseException
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter


class CsvAbstractReader(object):
    """
    Reads data from S3 CSV file
    """

    BUFFER_SIZE = 8192
    headerReportHeaders = ["Error type", "Header name"]

    def openFile(self,region,bucket,filename,csvSchema,bucketName,errorFilename):
        """ Opens file and prepares to read each record, mapping entries to specified column names
        Args:
            region: AWS region where the bucket is located (not used if instantiated as CsvLocalReader)
            bucket: the S3 Bucket (not used if instantiated as CsvLocalReader)
            filename: The file path for the CSV file in S3
            csvSchema: list of FileColumn objects for this file type
            bucketName: bucket to send errors to
            errorFilename: filename for error report
        """

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
            # Write header error for no header row
            with self.getWriter(bucketName, errorFilename, ["Error Type"], self.isLocal) as writer:
                writer.write(["No header row"])
                writer.finishBatch()
            raise ResponseException("CSV file must have a header",StatusCode.CLIENT_ERROR,ValueError,ValidationError.singleRow)

        duplicatedHeaders = []
        #create the header

        # check delimiters in header row
        pipeCount = line.count("|")
        commaCount = line.count(",")

        if pipeCount != 0 and commaCount != 0:
            # Write header error for mixed delimiter use
            with self.getWriter(bucketName, errorFilename, ["Error Type"], self.isLocal) as writer:
                writer.write(["Cannot use both ',' and '|' as delimiters. Please choose one."])
                writer.finishBatch()
            raise ResponseException("Error in header row: CSV file must use only '|' or ',' as the delimiter", StatusCode.CLIENT_ERROR, ValueError, ValidationError.headerError)

        self.delimiter = "|" if line.count("|") != 0 else ","

        validation_db = ValidationInterface()
        longNameDict = validation_db.getLongToShortColname()
        # Set the list of possibleFields, using  the shorter,
        # machine-readable column names
        possibleFields = {}
        for schema in csvSchema:
            possibleFields[FieldCleaner.cleanString(schema.name_short)] = 0

        for row in csv.reader([line], dialect='excel', delimiter=self.delimiter):
            # check to see if header contains long or short column names
            colMatches = 0
            for value in row:
                if FieldCleaner.cleanString(value) in longNameDict:
                    colMatches += 1
            # if most of column headers are in the long format,
            # we'll treat the file as having long headers
            if colMatches > .5 * len(row):
                longHeaders = True
            else:
                longHeaders = False

            for cell in row:
                submittedHeaderValue = FieldCleaner.cleanString(cell)
                if longHeaders and submittedHeaderValue in longNameDict:
                    headerValue = FieldCleaner.cleanString(longNameDict[submittedHeaderValue])
                elif longHeaders:
                    headerValue = None
                else:
                    headerValue = submittedHeaderValue
                if not headerValue in possibleFields:
                    # Allow unexpected headers, just mark the header as None so we skip it when reading
                    self.headerDictionary[current] = None
                    current += 1
                elif(possibleFields[headerValue] == 1):
                    # Add header value (as submitted) to duplicated header list
                    duplicatedHeaders.append(submittedHeaderValue)
                else:
                    self.headerDictionary[current] = headerValue
                    possibleFields[headerValue] = 1
                    current += 1
        self.columnCount = current
        #Check that all required fields exists
        missingHeaders = []
        for schema in csvSchema :
            if (possibleFields[FieldCleaner.cleanString(schema.name_short)] == 0):
                # return long colname for error reporting
                missingHeaders.append(schema.name)
        if(len(missingHeaders) > 0 or len(duplicatedHeaders) > 0):
            # Write header errors if any occurred and raise a header_error exception
            errorString = ""
            with self.getWriter(bucketName, errorFilename, self.headerReportHeaders, self.isLocal) as writer:
                extraInfo = {}
                if(len(duplicatedHeaders) > 0):
                    errorString = "".join([errorString, "Duplicated: ",", ".join(duplicatedHeaders)])
                    extraInfo["duplicated_headers"] = ", ".join(duplicatedHeaders)
                    for header in duplicatedHeaders:
                        writer.write(["Duplicated header", header])
                if(len(missingHeaders) > 0):
                    if(len(duplicatedHeaders)):
                        # Separate missing and duplicated headers if both are present
                        errorString += "| "
                    errorString = "".join([errorString, "Missing: ",", ".join(missingHeaders)])
                    extraInfo["missing_headers"] = ", ".join(missingHeaders)
                    for header in missingHeaders:
                        writer.write(["Missing header", header])
                writer.finishBatch()
            raise ResponseException("Errors in header row: " + str(errorString), StatusCode.CLIENT_ERROR, ValueError,ValidationError.headerError,**extraInfo)

        return longHeaders

    @staticmethod
    def getWriter(bucketName,fileName,header,isLocal, region = None):
        """
        Gets the write type based on if its a local install or not.
        """
        if(isLocal):
            return CsvLocalWriter(fileName,header)
        if region == None:
            region = CONFIG_BROKER["aws_region"]
        return CsvS3Writer(region, bucketName,fileName,header)

    def getNextRecord(self):
        """
        Read the next record into a dict and return it
        Returns:
            dictionary representing this record
        """
        returnDict = {}
        line = self._getLine()

        for row in csv.reader([line],dialect='excel', delimiter=self.delimiter):
            if len(row) != self.columnCount:
                raise ResponseException("Wrong number of fields in this row", StatusCode.CLIENT_ERROR, ValueError, ValidationError.readError)
            for current, cell in enumerate(row):
                if(current >= self.columnCount) :
                    raise ResponseException("Record contains too many fields",StatusCode.CLIENT_ERROR,ValueError,ValidationError.readError)
                if(cell == ""):
                    # Use None instead of empty strings for sqlalchemy
                    cell = None
                # self.headerDictionary uses the short, machine-readable column names
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
        raise NotImplementedError("Do not instantiate csvAbstractReader directly.")

    def _getFileSize(self):
        """
        Gets the size of the file
        """
        raise NotImplementedError("Do not instantiate csvAbstractReader directly.")

    def _getNextPacket(self):
        """
        Gets the next packet from the file returns true if successful
        """
        raise NotImplementedError("Do not instantiate csvAbstractReader directly.")

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
                current = "".join([current,char])
        if (len(current)>0) :
            linesToReturn.append(current)
        return linesToReturn
