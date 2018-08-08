from werkzeug.exceptions import BadRequest


class RequestDictionary:
    """ Provides an interface to an http request """

    def __init__(self, request, optional_request=False):
        self.requestDict = self.derive(request, optional_request)

    def get_value(self, value):
        """ Returns value for specified key """
        if value not in self.requestDict:
            raise ValueError(value + " not found")
        return self.requestDict[value]

    def exists(self, value):
        """ Returns True if key is in request json """
        if value not in self.requestDict:
            return False
        return True

    def to_string(self):
        return str(self.requestDict)

    @staticmethod
    def derive(request, optional_request=False):
        """Check request header to determine where to fetch arguments from.
        Raise exceptions. @todo - replace this whole class with standard flask
        HTTP argument handling"""
        try:
            if "Content-Type" not in request.headers:
                raise ValueError("Must include Content-Type header")
            content_type = request.headers['Content-Type']

            # Allowing extra content after application/json for Firefox compatibility
            if request.is_json:
                result = request.get_json()
                if not isinstance(result, dict):
                    # @todo: this shouldn't be a type error
                    raise TypeError("Failed to create a dictionary out of json")
                return result
            elif content_type == "application/x-www-form-urlencoded":
                return request.form
            # This is not common and is a one-off solution for inbound API
            elif "multipart/form-data" in content_type:
                request_data = request.form.to_dict()
                request_data['_files'] = request.files
                for key, value in request_data.items():
                    if str(value).upper() in ('NULL', 'NONE'):
                        request_data[key] = None
                return request_data
            else:
                raise ValueError("Invalid Content-Type : " + content_type)
        except BadRequest as br:
            if optional_request:
                return {}
            else:
                raise br
