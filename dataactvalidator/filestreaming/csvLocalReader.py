import os
from dataactvalidator.filestreaming.csvAbstractReader import CsvAbstractReader


class CsvLocalReader(CsvAbstractReader):

    def open_file(self, region, bucket, filename, csv_schema, bucket_name, error_filename, long_to_short_dict):
        """ Opens file and prepares to read each record, mapping entries to specified column names

        Args:
            region: Not used, included here to match signature of CsvAbstractReader.openFile
            bucket: Not used, included here to match signature CsvAbstractReader.openFile
            filename: The file path for the CSV file in S3
            csv_schema: list of FileColumn objects for this file type
            bucket_name: bucket to send errors to
            error_filename: filename for error report
            long_to_short_dict: mapping of long to short schema column names
        """
        self.filename = filename
        self.is_local = True
        try:
            self.file = open(filename,"r")
        except:
            raise ValueError("".join(["Filename provided not found : ", str(self.filename)]))
        super(CsvLocalReader,self).open_file(
            region, bucket, filename, csv_schema, bucket_name, error_filename, long_to_short_dict)

    def close(self):
        """Closes file if it exists """
        try:
            self.file.close()
        except AttributeError:
            # File does not exist, and so does not need to be closed
            pass

    def _get_file_size(self):
        """
        Gets the size of the file
        """
        return os.path.getsize(self.filename)

    def _get_next_packet(self):
        """
        Gets the next packet from the file returns true if successful
        """
        packet = self.file.read(CsvAbstractReader.BUFFER_SIZE)
        success = True
        if packet == "":
            success = False
        return success, packet
