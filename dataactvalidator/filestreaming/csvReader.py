import csv
import os
import tempfile
import boto3
from collections import OrderedDict

from dataactcore.config import CONFIG_BROKER
from dataactcore.utils.ResponseError import ResponseError
from dataactcore.utils.statusCode import StatusCode
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactcore.utils.stringCleaner import StringCleaner
from dataactvalidator.validation_handlers.validationError import ValidationError


class CsvReader(object):
    """
    Reads data from local CSV file (first downloading from S3 if necessary)
    """

    header_report_headers = ["Error type", "Header name"]

    def get_filename(self, region, bucket, filename):
        """Creates a filename based on the file path
        Args:
            region: AWS region where the bucket is located
            bucket: Optional parameter; if set, file will be retrieved from S3
            filename: The file path for the CSV file (local or in S3)
        """
        self.has_tempfile = False
        self.filename = filename

        if region and bucket:
            # If this is a file in S3, download to a local temp file first
            # Use temp file as local file
            (file, file_path) = tempfile.mkstemp()
            self.has_tempfile = True

            s3 = boto3.client("s3", region_name=region)
            s3.download_file(bucket, filename, file_path)
            self.filename = file_path

        return self.filename

    def open_file(
        self,
        region,
        bucket,
        filename,
        csv_schema,
        bucket_name,
        error_filename,
        gsdm_to_short_dict,
        short_to_gsdm_dict,
        is_local=False,
    ):
        """Opens file and prepares to read each record, mapping entries to specified column names

        Args:
            region: AWS region where the bucket is located
            bucket: Optional parameter; if set, file will be retrieved from S3
            filename: The file path for the CSV file (local or in S3)
            csv_schema: list of FileColumn objects for this file type
            bucket_name: bucket to send errors to
            error_filename: filename for error report
            gsdm_to_short_dict: mapping of GSDM to short schema column names
            short_to_gsdm_dict: mapping of short to GSDM schema column names
            is_local: Boolean of whether the app is being run locally or not
        """

        if not self.filename:
            self.get_filename(region, bucket, filename)

        self.is_local = is_local
        try:
            self.file = open(self.filename, "r", newline=None)
        except Exception:
            raise ValueError("".join(["Filename provided not found : ", str(self.filename)]))

        self.extra_line = False
        self.is_finished = False
        self.column_count = 0
        header_line = self.file.readline()
        # make sure we have not finished reading the file

        if self.is_finished:
            # Write header error for no header row
            self.write_file_level_error(bucket_name, error_filename, ["Error Type"], ["No header row"], self.is_local)
            raise ResponseError(
                "CSV file must have a header", StatusCode.CLIENT_ERROR, ValueError, ValidationError.single_row
            )

        self.delimiter = "|" if header_line.count("|") > header_line.count(",") else ","
        self.csv_reader = csv.reader(self.file, quotechar='"', dialect="excel", delimiter=self.delimiter)
        self.header_dict = {}

        # create the header
        header_row = next(csv.reader([header_line], quotechar='"', dialect="excel", delimiter=self.delimiter))
        gsdm_headers = use_gsdm_headers(header_row, gsdm_to_short_dict)
        header_row = list(normalize_headers(header_row, gsdm_headers, gsdm_to_short_dict, self.header_dict))
        # Storing the flex fields for easy access
        self.flex_fields = [header for header in header_row if header.startswith("flex_")]

        expected_header_counts = self.count_and_set_headers(csv_schema, header_row)

        self.column_count = len(header_row)

        self.handle_missing_duplicate_headers(expected_header_counts, bucket_name, error_filename, short_to_gsdm_dict)

        return gsdm_headers

    @staticmethod
    def write_file_level_error(bucket_name, filename, header, error_content, is_local):
        """Writes file-level errors to an error file

        Args:
            bucket_name: Name of the S3 bucket to write to if not local
            filename: Name (including path) of the file to write
            header: The header line for the file
            error_content: list of lines representing content for the error file
            is_local: boolean indicating if the file is to be written locally or to S3
        """
        if is_local:
            with CsvLocalWriter(filename, header) as writer:
                for line in error_content:
                    if isinstance(line, str):
                        writer.write([line])
                    else:
                        writer.write(line)
                writer.finish_batch()
        else:
            s3client = boto3.client("s3", region_name=CONFIG_BROKER["aws_region"])
            # add headers
            contents = bytes((",".join(header) + "\n").encode())
            for line in error_content:
                if isinstance(line, str):
                    contents += bytes((line + "\n").encode())
                else:
                    contents += bytes((",".join(line) + "\n").encode())
            s3client.put_object(Bucket=bucket_name, Key=filename, Body=contents)

    def handle_missing_duplicate_headers(self, expected_fields, bucket_name, error_filename, short_to_gsdm_dict):
        """Check for missing or duplicated headers. If present, raise an exception with a meaningful message.

        Args:
            expected_fields: a list of expected column headers
            bucket_name: the name of the S3 bucket to write the error to
            error_filename: the name of the error (including path) file to write to
            short_to_gsdm_dict: mapping of short to GSDM schema column names
        """
        missing_headers = [short_to_gsdm_dict[cell] for cell, count in expected_fields.items() if count == 0]
        duplicated_headers = [short_to_gsdm_dict[cell] for cell, count in expected_fields.items() if count > 1]

        if missing_headers or duplicated_headers:
            self.write_missing_duplicated_headers(missing_headers, duplicated_headers, bucket_name, error_filename)
            raise_missing_duplicated_exception(missing_headers, duplicated_headers)

    def write_missing_duplicated_headers(self, missing_headers, duplicated_headers, bucket_name, error_filename):
        """Write duplicate or missing header errors if any occurred and raise a header_error exception.

        Args:
            missing_headers: an array of the names of the missing headers
            duplicated_headers: an array of the names of the duplicated headers
            bucket_name: the name of the S3 bucket to write the error to
            error_filename: the name of the error (including path) file to write to
        """
        error_text = []
        for header in duplicated_headers:
            error_text.append(["Duplicated header", header])
        for header in missing_headers:
            error_text.append(["Missing header", header])
        self.write_file_level_error(bucket_name, error_filename, self.header_report_headers, error_text, self.is_local)

    def count_and_set_headers(self, csv_schema, header_row):
        """Track how many times we've seen a field we were expecting and set self.expected_headers and
        self.flex_headers

        Args:
            csv_schema: list of FileColumn objects for this file type
            header_row: an array of the file headers given

        Returns:
            expected field dict {[expected field name]: [header count])
        """
        self.expected_headers = []
        self.flex_headers = []

        # Track how many times we've seen a field we were expecting. Keyed by the shorter, machine-readable column names
        expected_fields = OrderedDict()

        for schema in csv_schema:
            expected_fields[FieldCleaner.clean_name(schema.name_short)] = 0

        for header_value in header_row:
            if header_value not in expected_fields:
                # Add flex headers to flex list
                if str(header_value).startswith("flex_"):
                    self.flex_headers.append(header_value)
                else:
                    self.flex_headers.append(None)
                # Allow unexpected headers, just mark the header as None so we skip it when reading
                self.expected_headers.append(None)
            else:
                self.flex_headers.append(None)
                self.expected_headers.append(header_value)
                expected_fields[header_value] += 1
        return expected_fields

    def close(self):
        """Closes file if it exists"""
        try:
            self.file.close()
            if self.has_tempfile:
                os.remove(self.filename)
        except AttributeError:
            # File does not exist, and so does not need to be closed
            pass


def use_gsdm_headers(header_row, gsdm_to_short_dict):
    """Check to see if header contains GSDM or short column names

    Args:
        header_row: an array of the file headers given
        gsdm_to_short_dict: a dictionary containing a mapping from GSDM headers to short ones for this file type

    Returns:
        bool representing whether to use GSDM or short column names (True for GSDM)
    """
    col_matches = 0
    for value in header_row:
        if StringCleaner.clean_string(value, remove_extras=False) in gsdm_to_short_dict:
            col_matches += 1
    # if most of column headers are in the long format, we'll treat the file as having long headers
    return col_matches > 0.5 * len(header_row)


def normalize_headers(header_row, gsdm_headers, gsdm_to_short_dict, header_dict):
    """Clean the headers (lowercase) and convert them to short headers if we're given long headers

    Args:
        header_row: an array of the file headers given
        gsdm_headers: boolean indicating if we're using the GSDM versions of the headers (True for GSDM)
        gsdm_to_short_dict: a dictionary containing a mapping from GSDM headers to short ones for this file type
        header_dict: a dictionary containing the mappings from original to cleaned headers

    Yields:
        A string containing the cleaned header name (converted to short version if GSDM versions were provided and
        there is a mapping for that header).
    """
    for original_header in header_row:
        header = StringCleaner.clean_string(original_header, remove_extras=False)
        # Replace headers that don't match DB but are allowed by the broker with their DB matches
        if header == "deobligationsrecoveriesrefundsofprioryearbyprogramobjectclass_cpe":
            header = "deobligationsrecoveriesrefundsdofprioryearbyprogramobjectclass_cpe"
        elif header == "facevalueloanguarantee":
            header = "facevalueofdirectloanorloanguarantee"
        elif header == "budgetauthorityavailableamounttotal_cpe":
            header = "totalbudgetaryresources_cpe"
        elif header == "correctionlatedeleteindicator":
            header = "correctiondeleteindicator"
        elif header == "place_of_performance_zip4":
            header = "place_of_performance_zip4a"
        elif header == "deobligationsrecoveriesrefundsbytas_cpe":
            header = "deobligationsrecoveriesrefundsofprioryearbytas_cpe"
        elif header == "cfda_number":
            header = "assistancelistingnumber"

        # yield the short header when applicable, otherwise yield the cleaned header, whatever it is
        if gsdm_headers and header in gsdm_to_short_dict:
            header_dict[original_header] = FieldCleaner.clean_name(gsdm_to_short_dict[header])
            yield FieldCleaner.clean_name(gsdm_to_short_dict[header])
        else:
            header_dict[original_header] = header
            yield header


def raise_missing_duplicated_exception(missing_headers, duplicated_headers):
    """Construct and raise an exception about missing and/or duplicated headers"""
    error_string, extra_info = "", {}
    duplicated_str = ", ".join(duplicated_headers)
    missing_str = ", ".join(missing_headers)
    if duplicated_str:
        error_string = "Duplicated: " + duplicated_str
        extra_info["duplicated_headers"] = duplicated_str
    if missing_str:
        error_string = "Missing: " + missing_str
        extra_info["missing_headers"] = missing_str

    if error_string:
        raise ResponseError(
            "Errors in header row: " + str(error_string),
            StatusCode.CLIENT_ERROR,
            ValueError,
            ValidationError.header_error,
            **extra_info,
        )
