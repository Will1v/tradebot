#!/usr/bin/env python

from cexio_interface import CexioInterface
from logger import get_logger
import time

safe_path = "../safe/cex_read_only_credentials.txt"

def get_credentials():
    # import Read-only credentials from config file
    credentials = {}
    for l in open(safe_path).readlines():
        split = l.replace('\n', '').split('=')
        credentials[split[0]] = split[1]
    return credentials


if __name__ == "__main__":

    cexio_logger = get_logger('Cexio')

    cred = get_credentials()

    cmdh = CexioMarketDataHandler(cred['key'], cred['secret'], cexio_logger)
#    assert cmdh
    assert ci
#    cmdh.start()
    ci.start()
    time.sleep(3)
#    cmdh.subscribe_ticker()
#    cmdh.connect()
    while True:
        time.sleep(60)
        cmdh.update_balance()
