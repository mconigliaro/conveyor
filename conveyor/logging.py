from __future__ import absolute_import

import logging

from .options import options


# get logger
log = logging.getLogger()

# set log level
log.setLevel(getattr(logging, options.log_level.upper()))

# set log format
log_format = logging.Formatter("%(asctime)s:%(process)d:%(levelname)s@%(funcName)s@%(lineno)d: %(message)s")

# configure console logger
console_logger = logging.StreamHandler()
console_logger.setFormatter(log_format)
log.addHandler(console_logger)

# configure file logger
if options.log_file_path != None:
    zookeeper.set_log_stream(options.log_file_path)

    fileLogger = logging.handlers.TimedRotatingFileHandler(
        filename = options.log_file_path,
        when = options.log_file_rotate_interval_type,
        interval = options.log_file_rotate_interval,
        backupCount = options.log_file_max_backups)
    fileLogger.setFormatter(log_format)
    log.addHandler(fileLogger)
