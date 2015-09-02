
INFO = 0x01
WARNING = 0x02
ERROR = 0x04
DEBUG = 0x08
ALL = 0x0FF

_log_levels = ALL
_custom_logger = None
_verbose = False

def log(type, message):
    header = ""
    if type == 'i':
        if _log_levels & INFO == 0:
            return
        header = "[INFO] "
    elif type == 'e':
        if _log_levels & ERROR == 0:
            return
        header = "[ERROR]"
    elif type == 'w':
        if _log_levels & WARNING == 0:
            return
        header = "[WARN] "
    elif type == 'd':
        if _log_levels & DEBUG == 0:
            return
        header = "[DEBUG]"

    text = "%s %s" % (header, message)

    if _custom_logger is not None:
        if _verbose:
            print(text)
        _custom_logger(text)
    else:
        print(text)


def set_logger(logger):
    global _custom_logger
    _custom_logger = logger

def set_verbose(verbose):
    global _verbose
    _verbose = verbose


def set_levels(levels):
    global _log_levels
    _log_levels = levels