import logging
from logging.handlers import RotatingFileHandler


def get_logger(name: str, level=logging.INFO, log_to_file=False, log_file="app.log"):
    """
    Creates and returns a logger with standardized settings.

    :param name: Logger name (typically __name__)
    :param level: Logging level (default: INFO)
    :param log_to_file: Whether to log to a rotating file
    :param log_file: Log file name if file logging is enabled
    :return: Configured logger instance
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # Avoid duplicate handlers

    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Optional file handler
    if log_to_file:
        file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
