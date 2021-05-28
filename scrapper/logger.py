import logging
import sys
import datetime


def get_logger():
    # create logger
    logger = logging.getLogger('scrapper')
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)

    # create file handler and set level to debug
    today = datetime.datetime.now().isoformat()[:10]
    fh = logging.FileHandler(f"scrapper/logs/{today}.log")
    fh.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(message)s')

    # add formatter to ch and fh
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger
