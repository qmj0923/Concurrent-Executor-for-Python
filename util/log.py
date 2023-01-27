import logging
import os
import time


def setup_custom_logger(log_file_name):
    os.makedirs(os.path.dirname(log_file_name), exist_ok=True)
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
    fileHandler = logging.FileHandler(log_file_name, mode='a')
    fileHandler.setFormatter(formatter)

    logger = logging.getLogger(log_file_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(fileHandler)
    logger.addHandler(logging.StreamHandler())  # Write to stdout as well
    time_ = time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime())
    logger.info(f"Start logging: {time_}")

    return logger
