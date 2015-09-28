import time

class Logger():
    NONE = 0
    INFO = 0x01
    WARNING = 0x02
    ERROR = 0x04
    DEBUG = 0x08
    ALL = 0x0FF

    _logs = []

    def __init__(self):
        self._levels = Logger.ALL  # all by default
        self._verbose = False
        self._custom_logger = None
        Logger._logs.append(self)

    def __del__(self):
        Logger._logs.remove(self)

    @staticmethod
    def global_set_levels(levels):
        for log in Logger._logs:
            log.set_levels(levels)

    @staticmethod
    def global_set_logger(custom_logger):
        for log in Logger._logs:
            log.set_logger(custom_logger)

    @staticmethod
    def global_set_verbose(verbose):
        for log in Logger._logs:
            log.set_verbose(verbose)



    def set_levels(self, levels):
        self._levels = levels

    def set_logger(self, custom_logger):
        self._custom_logger = custom_logger

    def set_verbose(self, verbose):
        self._verbose = verbose

    def info(self, message):
        self._log(Logger.INFO, message)

    def warning(self, message):
        self._log(Logger.WARNING, message)

    def error(self, message):
        self._log(Logger.ERROR, message)

    def debug(self, message):
        self._log(Logger.DEBUG, message)

    def _log(self, type, message):
        if (type & self._levels) == 0:
            return

        timestamp = time.strftime("%Y/%m/%d %H:%M:%S")
        type_str = "[?????]"
        if type is Logger.INFO:
            type_str = "[INFO] "
        elif type is Logger.WARNING:
            type_str = "[WARN] "
        elif type is Logger.ERROR:
            type_str = "[ERROR]"
        elif type is Logger.DEBUG:
            type_str = "[DEBUG]"

        text = "%s %s %s" % (timestamp, type_str, message)

        if self._custom_logger is not None:
            if self._verbose:
                print(text)
            self._custom_logger(text)
        else:
            print(text)
