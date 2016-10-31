import os
from dataactvalidator.filestreaming.csvAbstractReader import CsvAbstractReader


class CsvLocalReader(CsvAbstractReader):


    def openFile(self,region,bucket,filename,csvSchema,bucketName,errorFilename):
        """ Opens file and prepares to read each record, mapping entries to specified column names

        Args:
            region: Not used, included here to match signature of CsvAbstractReader.openFile
            bucket: Not used, included here to match signature CsvAbstractReader.openFile
            filename: The file path for the CSV file in S3
            csvSchema: list of FileColumn objects for this file type
            bucketName: bucket to send errors to
            errorFilename: filename for error report
        """
        self.filename = filename
        self.isLocal = True
        try:
            self.file = open(filename,"r")
        except :
            raise ValueError("".join(["Filename provided not found : ",str(self.filename)]))
        super(CsvLocalReader,self).openFile(region,bucket,filename,csvSchema,bucketName,errorFilename)

    def close(self):
        """Closes file if it exists """
        try:
            self.file.close()
        except AttributeError:
            # File does not exist, and so does not need to be closed
            pass

    def _getFileSize(self):
        """
        Gets the size of the file
        """
        return os.path.getsize(self.filename)

    def _getNextPacket(self):
        """
        Gets the next packet from the file returns true if successful
        """
        packet  = self.file.read(CsvAbstractReader.BUFFER_SIZE)
        success = True
        if(packet == ""):
            success = False
        return success,packet
