import logging
import sys, os
from datetime import datetime


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)


#    filepath = "../logs/{}_{}.log".format(logger_name, datetime.now().strftime('%Y%m%d_%H%M'))
    filepath = "../logs/{}_{}.log".format(logger_name, datetime.now().strftime('%Y%m%d_%H%M'))
    if os.path.isfile(filepath):
        print "Logfile for {} already existing, will try to delete it...".format(logger_name)
        os.remove(filepath)
        print "Logfile for {} deleted".format(logger_name)

    handler = logging.FileHandler(filepath)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    std_out_handler = logging.StreamHandler(sys.stdout)
    std_out_handler.setLevel(logging.DEBUG)
    std_out_handler.setFormatter(formatter)
    logger.addHandler(std_out_handler)

    logger.debug('Logger {} initiated'.format(logger_name))

    return logger

