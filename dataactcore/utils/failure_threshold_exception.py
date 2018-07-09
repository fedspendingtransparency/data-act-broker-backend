class FailureThresholdExceededException(Exception):
    def __init__(self, count):
        """ Count should be the raw value that exceeded the threshold """
        self.count = count
