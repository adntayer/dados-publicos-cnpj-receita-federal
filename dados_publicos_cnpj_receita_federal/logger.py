import logging


class SetupLogger:
    formatting = '[%(levelname)7s - %(asctime)s] [%(filename)s:%(name)s] | %(message)s'

    def __init__(self, logger_name, format=formatting, level=logging.INFO):
        logging.basicConfig(
            format=format,
            level=level,
            handlers=[
                logging.StreamHandler(),
            ],
        )
        self.log = logging.getLogger(logger_name)

    def critical(self, msg):
        self.log.critical(msg)

    def error(self, msg):
        self.log.error(msg)

    def warning(self, msg):
        self.log.warning(msg)

    def info(self, msg):
        self.log.info(msg)

    def debug(self, msg):
        self.log.debug(msg)
