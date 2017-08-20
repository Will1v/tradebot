#!/usr/bin/env python

from cexio_interface import CexioMarketDataHandler, CexioTraderBot
from db_interface import CrateDbInterface
from collections import OrderedDict

from logger import get_logger
import time

safe_path = "../safe/cex_read_only_credentials.txt"
config_path = "../safe/config.txt"


# import Read-only credentials from config file
def get_credentials():
    credentials = {}
    for l in open(safe_path).readlines():
        split = l.replace('\n', '').split('=')
        credentials[split[0]] = split[1]
    return credentials

def get_config():
    config = OrderedDict()
    for l in open(config_path).readlines():
        split = l.replace('\n', '').split('=')
        config[split[0]] = split[1]
    return config

def set_config(config):
    file = open(config_path, "w")
    for k, v in config.iteritems():
        file.write("{}={}".format(k, v))


# Inits DB
# If reset = true, drops and recreates all tables
def init_db():
    logger = get_logger("Tradebot")

    crate_logger = get_logger('CrateDB')
    db = CrateDbInterface(crate_logger)
    logger.info("CrateDbInterface instantiated")

    config = get_config()
    if config['reset_db_at_startup'] == "True":
        logger.info("DB Reset requested")
        db.drop_and_create_db()
        config['reset_db_at_startup'] = False
        set_config(config)
        logger.debug("DB has been reset, parameter reset_db_at_startup set back to False")


    logger.info("Crate DB initiated successfuly")
    return db


if __name__ == "__main__":

    crate_interface = init_db()

    cexio_logger = get_logger('Cexio')
    cred = get_credentials()

    cmdh = CexioMarketDataHandler(cred['key'], cred['secret'], crate_interface, cexio_logger)
    assert cmdh
    cmdh.subscribe_orderbook('BTC', 'USD', 2)

    while True:
        time.sleep(60)
#         cmdh.update_balance()
