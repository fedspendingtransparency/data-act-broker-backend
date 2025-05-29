class ValidationError:
    """This class holds errors that can occur during validation, for use in the error report and database"""

    type_error_msg = (
        "The value provided was of the wrong type. Note that all type errors in a line"
        " must be fixed before the rest of the validation logic is applied to that line."
    )
    type_error = 0
    required_error_msg = "This field is required for all submissions but was not provided in this row."
    required_error = 1
    value_error_msg = "The value provided was invalid."
    value_error = 2
    header_error_msg = "The file has errors in the header row."
    header_error = 3
    read_error_msg = "Could not parse this record correctly."
    read_error = 4
    write_error_msg = "Could not write this record into the staging table."
    write_error = 5
    unknown_error_msg = "An unknown error has occurred with this file."
    unknown_error = 6
    single_row_msg = "CSV file must have a header row and at least one record."
    single_row = 7
    job_error_msg = "Error occurred in job manager."
    job_error = 8
    length_error_msg = "Value was longer than maximum length for this field."
    length_error = 9
    encoding_error_msg = "File contains invalid characters that could not be validated."
    encoding_error = 10
    row_count_error_msg = "Raw file row count does not match the number of rows validated."
    row_count_error = 11
    file_type_error_msg = "Invalid file type. Valid file types include .csv and .txt."
    file_type_error = 12
    field_format_error_msg = "Date should follow the YYYYMMDD format."
    field_format_error = 13
    blank_file_error_msg = (
        "File does not contain data. For files A and B, this must be addressed prior to"
        " publication/certification. Blank file C does not prevent publication/certification."
    )
    blank_file_error = 14
    # Create dict of error types
    error_dict = {
        type_error: type_error_msg,
        required_error: required_error_msg,
        value_error: value_error_msg,
        header_error: header_error_msg,
        read_error: read_error_msg,
        write_error: write_error_msg,
        unknown_error: unknown_error_msg,
        single_row: single_row_msg,
        job_error: job_error_msg,
        length_error: length_error_msg,
        encoding_error: encoding_error_msg,
        row_count_error: row_count_error_msg,
        file_type_error: file_type_error_msg,
        field_format_error: field_format_error_msg,
        blank_file_error: blank_file_error_msg,
    }
    error_type_dict = {
        type_error: "type_error",
        required_error: "required_error",
        value_error: "value_error",
        header_error: "header_error",
        read_error: "read_error",
        write_error: "write_error",
        unknown_error: "unknown_error",
        single_row: "single_row_error",
        job_error: "job_error",
        length_error: "length_error",
        encoding_error: "encoding_error",
        row_count_error: "row_count_error",
        file_type_error: "file_type_error",
        field_format_error: "field_format_error",
        blank_file_error: "blank_file_error",
    }

    @staticmethod
    def get_error_message(error_type):
        """Retrieve error message for specified error type"""
        if error_type is None:
            # If no error type is provided, this is an unknown error
            error_type = ValidationError.unknown_error
        if error_type in ValidationError.error_dict:
            return ValidationError.error_dict[error_type]
        else:
            # Not a valid error type
            raise ValueError("Called get_error_message with an invalid error type: " + str(error_type))

    @staticmethod
    def get_error_type_string(error_type):
        """Get string identifier used in database for specified error type"""
        if error_type is None:
            # If no error type is provided, this is an unknown error
            error_type = ValidationError.unknown_error
        if error_type in ValidationError.error_type_dict:
            return ValidationError.error_type_dict[error_type]
        else:
            # Not a valid error type
            raise ValueError("Called get_error_type_string with an invalid error type: " + str(error_type))
