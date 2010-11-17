import logging

from options import options
import zkplus


# get logger
log = logging.getLogger()

# set log level
if options.log_level != None:
    options.log_level = options.log_level.upper()
    if options.log_level.startswith('C'):
        options.log_level = 'CRITICAL'
        log.setLevel(logging.CRITICAL)
        zkplus.set_debug_level(0)
    elif options.log_level.startswith('E'):
        options.log_level = 'ERROR'
        log.setLevel(logging.ERROR)
        zkplus.set_debug_level(0)
        #zkplus.set_debug_level(zkplus.LOG_LEVEL_ERROR)
    elif options.log_level.startswith('W'):
        options.log_level = 'WARNING'
        log.setLevel(logging.WARNING)
        zkplus.set_debug_level(0)
        #zkplus.set_debug_level(zkplus.LOG_LEVEL_WARN)
    elif options.log_level.startswith('I'):
        options.log_level = 'INFO'
        log.setLevel(logging.INFO)
        zkplus.set_debug_level(0)
        #zkplus.set_debug_level(zkplus.LOG_LEVEL_INFO)
    elif options.log_level.startswith('D'):
        options.log_level = 'DEBUG'
        log.setLevel(logging.DEBUG)
        zkplus.set_debug_level(zkplus.LOG_LEVEL_DEBUG)
else:
    log.setLevel(logging.NOTSET)
    zkplus.set_debug_level(0)

# set log format
log_format = logging.Formatter("%(asctime)s:%(process)d:%(levelname)s@%(funcName)s@%(lineno)d: %(message)s")

# configure console logger
console_logger = logging.StreamHandler()
console_logger.setFormatter(log_format)
log.addHandler(console_logger)

# configure file logger
if options.log_file_path != None:
    zkplus.set_log_stream(options.log_file_path)

    fileLogger = logging.handlers.TimedRotatingFileHandler(
        filename = options.log_file_path,
        when = options.log_file_rotate_interval_type,
        interval = options.log_file_rotate_interval,
        backupCount = options.log_file_max_backups)
    fileLogger.setFormatter(log_format)
    log.addHandler(fileLogger)
