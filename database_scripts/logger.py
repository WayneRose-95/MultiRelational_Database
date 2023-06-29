import os
import logging

class DatabaseLogger:
    def __init__(self, log_filename):
        if not os.path.exists(log_filename):
            os.makedirs(os.path.dirname(log_filename), exist_ok=True)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # Format the logs by time, filename, function_name, level_name, and the message
        formatter = logging.Formatter("%(asctime)s:%(filename)s:%(funcName)s:%(levelname)s:%(message)s")
        file_handler = logging.FileHandler(log_filename)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def info(self, message):
        self.logger.info(message)

    def exception(self, message):
        self.logger.exception(message)

    def warning(self, message):
        self.logger.warning(message)

    def critical(self, message):
        self.logger.critical(message)

    def debug(self, message):
        self.logger.debug(message)

    def error(self, message):
        self.logger.error(message)



