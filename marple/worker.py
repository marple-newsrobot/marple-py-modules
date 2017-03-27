# encoding: utf-8

class Worker(object):
    """Represents a module that performs a series of tasks, such as
    robotreporter and data importer"""
    def __init__(self, logger=None):
        self._logger = None

    @property
    def log(self):
        """Use this method for logging. Specify the logger module
            upon initialization by setting defining self._logger.
            A worker is silent by default.
        """
        if not hasattr(self, "_logger"):
            self._logger = SilentLogger()
        if self._logger == None:
            self._logger = SilentLogger()

        return self._logger


class SilentLogger():
    """ Silent logger that does not produce any output 
    """

    def log(self, msg, *args, **kwargs):
        pass

    def debug(self, msg, *args, **kwargs):
        pass

    def info(self, msg, *args, **kwargs):
        pass

    def warning(self, msg, *args, **kwargs):
        pass

    def error(self, msg, *args, **kwargs):
        pass

    def critical(self, msg, *args, **kwargs):
        pass
        