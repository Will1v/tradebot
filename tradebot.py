#!/usr/bin/env python

from cexio_interface import CexioMarketDataHandler, CexioTraderBot
import db_interface

from logger import get_logger
import time

safe_path = "../safe/cex_read_only_credentials.txt"


# import Read-only credentials from config file
def get_credentials():
    credentials = {}
    for l in open(safe_path).readlines():
        split = l.replace('\n', '').split('=')
        credentials[split[0]] = split[1]
    return credentials

# Inits DB
# If reset = true, drops and recreates all tables
def init_db(reset):
    crate_logger = get_logger('CrateDB')

    db = db_interface.CrateDbInterface(crate_logger)
    if reset:
        db.drop_and_create_db()

    crate_logger.info("Crate DB initiated successfuly")
    return db


if __name__ == "__main__":

    crate_interface = init_db(True)

    cexio_logger = get_logger('Cexio')
    cred = get_credentials()

    cmdh = CexioMarketDataHandler(cred['key'], cred['secret'], crate_interface, cexio_logger)
    #cmdh = CexioMarketDataHandler(cred['key'], cred['secret'], cexio_logger)

    assert cmdh
#    assert ci
#    cmdh.start()
    cmdh.subscribe_orderbook('BTC', 'USD', 10)
    cmdh.subscribe_orderbook('ETH', 'USD', 3)

#     time.sleep(3)
# #    cmdh.subscribe_ticker()
# #    cmdh.connect()
    while True:
        time.sleep(60)
#         cmdh.update_balance()
