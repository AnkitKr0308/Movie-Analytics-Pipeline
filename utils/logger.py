import os
from datetime import datetime
import logging
from config import log_path


def write_log(log_dir=log_path):
    # exist_ok=True → If the folder already exists, it won’t throw an error.
    os.makedirs(log_dir, exist_ok=True)
    # strftime("%m_%d_%Y") -> formats the date in the required type.
    timestamp = datetime.now().strftime("%m%d%Y")
    # joins the path name in the log directory
    log_file = os.path.join(log_dir, f"log_{timestamp}.log")

    # Creates a logger object with the name "Movie_Pipeline".
    logger = logging.getLogger("Movie_Pipeline")
    # Sets minimum severity to log. All messages INFO and above (INFO, WARNING, ERROR, CRITICAL) will be captured.
    logger.setLevel(logging.INFO)
    logger.propagate = False  # avoid duplicate entries

    # Creates a file handler that writes logs to the file.
    fh = logging.FileHandler(log_file, mode="a")
    # Only log INFO and higher messages are written to this file.
    fh.setLevel(logging.INFO)

    # Creates a console handler so logs can be seen in real-time on the terminal.
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # logging.Formatter(...) → Defines the log message format.
    # %(asctime)s → Timestamp of the log entry.
    # %(name)s → Name of the logger (movie_pipeline).
    # %(levelname)s → Log level (INFO, WARNING, ERROR, etc.).
    # %(message)s → Actual log message you pass in logger.info().
    fh.setFormatter(formatter)  # Attach this format to file handler.
    ch.setFormatter(formatter)  # Attach this format to console handler.

    if not logger.handlers:
        logger.addHandler(fh)
        logger.addHandler(ch)
    # Ensures handlers are added only once.
    # Without this, multiple calls to write_log() would add duplicate handlers, causing the same log to appear twice.

    return logger
