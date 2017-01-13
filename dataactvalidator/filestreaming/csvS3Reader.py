import boto
from dataactvalidator.filestreaming.csvAbstractReader import CsvAbstractReader


class CsvS3Reader(CsvAbstractReader):
    """
    Reads data from S3 CSV file
    """

    def initialize_file(self, region, bucket, filename):
        """Returns an S3 filename."""
        s3connection = boto.s3.connect_to_region(region)
        s3_bucket = s3connection.lookup(bucket)
        if not s3_bucket:
            raise ValueError("Bucket {} not found in region {}".format(
                bucket, region))
        s3_file = s3_bucket.get_key(filename)
        if not s3_file:
            raise ValueError("Filename {} not found in bucket {}".format(
                filename, bucket))
        return s3_file

    def open_file(self, region, bucket, filename, csv_schema, bucket_name, error_filename, long_to_short_dict):
        """ Opens file and prepares to read each record, mapping entries to specified column names
        Args:
            region: AWS region where the bucket is located (not used if instantiated as CsvLocalReader)
            bucket: the S3 Bucket (not used if instantiated as CsvLocalReader)
            filename: The file path for the CSV file in S3
            csv_schema: list of FileColumn objects for this file type
            bucket_name: bucket to send errors to
            error_filename: filename for error report
            long_to_short_dict: mapping of long to short schema column names
        Returns:
        """
        self.s3_file = self.initialize_file(region, bucket, filename)
        self.is_local = False

        super(CsvS3Reader, self).open_file(
            region, bucket, filename, csv_schema, bucket_name, error_filename, long_to_short_dict)

    def close(self):
        """ Don't need to close file when streaming from S3 """
        pass

    def _get_file_size(self):
        """
        Gets the size of the file
        """
        return self.s3_file.size

    def _get_next_packet(self):
        """
        Gets the next packet from the file returns true if successful
        """
        offset_check = self.packet_counter * CsvAbstractReader.BUFFER_SIZE
        header = {'Range': 'bytes={}-{}'.format(
            offset_check, offset_check + CsvAbstractReader.BUFFER_SIZE - 1)}
        try:
            packet = self.s3_file.get_contents_as_string(headers=header, encoding='utf-8')
            return True, packet
        except:
            return False, ""
