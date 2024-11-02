import logging

class LogHandler:
    def logger(self):
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        return logger