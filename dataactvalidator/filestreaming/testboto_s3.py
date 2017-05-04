# import os
# import sys
# import threading
#

import os
import tempfile
import boto3


s3=boto3.client('s3')


(file, file_path) = tempfile.mkstemp()

with open(file_path, 'wb') as data:
    s3.download_fileobj('dev-data-act-submission', '10/1463507751_appropValid.csv', data)
    print(file_path)


