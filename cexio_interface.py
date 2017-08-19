import hmac
import hashlib
import datetime, time, sys
import json, yaml
import pdb
import logging
import threading
import thread
from tabulate import tabulate
from collections import OrderedDict

import websocket

web_socket_url = 'wss://ws.cex.io/ws/'

class CexioInterface(object):

    """
    Generic Cexio Interface class.
    Child classes:
        CexioMarketDataHandler
        CexioTraderBot
    """
    
    def __init__(self, key, secret, db_interface, cexio_logger):
#       type: (String, String, Logger) -> object
        self.key = key
        self.secret = secret
        self.logger = cexio_logger
#       Initialising connection to websocketi
        self.balance = None
        self.actions_on_msg_map = {
            "connected": self.connected_act,
            "ping": self.pong_act,
            "disconnecting": self.disconnecting_act,
            "auth": self.auth_act
            }
        self.is_connected = False
        self.db = db_interface
    
    def start(self):
        self.logger.info("Starting new {}".format(type(self)))
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(web_socket_url,
                                  on_message = self.on_message,
                                  on_error = self.on_error,
                                  on_close = self.on_close)
        self.ws.on_open = self.on_open
        thread.start_new_thread(self.ws.run_forever, ())
        time.sleep(1)

    def on_message(self, ws, message):
        msg = yaml.load(message)
        self.logger.info("[WS] on_message event, msg = {}".format(msg))
        if msg['e'] in self.actions_on_msg_map.keys():
            self.actions_on_msg_map[msg['e']](msg)
        else:
            self.logger.warning("Unknown message: {}, will be discarded".format(msg))

    def connected_act(self):
        self.is_connected = True
        self.logger.info("[WS] Websocket connected")

    def pong_act(self):
        self.logger.debug("Sending pong")
        self.ws.send(json.dumps({
            "e": "pong"
        }))

    def disconnecting_act(self):
        self.is_connected = False
        self.logger.info("[WS] Disconnecting...")

    def auth_act(self):
        self.logger.info("[WS] Authentified to exchange")

    def on_error(self, ws, error):
        self.logger.info("[WS] on_error event, err = {}".format(error))

    def on_close(self, ws):
        self.logger.info("[WS] on_close event, msg = {}".format(message))
        self.logger.warning("Websocket closed")

    def on_open(self, ws):
        def run(*args):
            self.connect()
        thread.start_new_thread(run, ())

    def get_timestamp(self):
        return int(time.time())

    def create_signature(self):  # (string key, string secret) 
        timestamp = self.get_timestamp()  # UNIX timestamp in seconds
        nonce = "{}{}".format(timestamp, self.key)
        self.logger.info("[WS] Creating signature with key = {} and nonce = {}".format(self.key, nonce))
        return timestamp, hmac.new(self.secret, nonce, hashlib.sha256).hexdigest()

    def auth_request(self):
        timestamp, signature = self.create_signature()

        return json.dumps({'e': 'auth',
                'auth': {'key': self.key, 'signature': signature, 'timestamp': timestamp,}, 'oid': 'auth', })

    def connect(self):
        self.logger.info("[WS] Connecting to websocket: " + web_socket_url)
        self.ws.send(self.auth_request())

class CexioMarketDataHandler(CexioInterface):
    """
    CexioMarketDataHandler allows to deal with all Market data tasks
    """
    def __init__(self, key, secret, db_interface, cexio_logger):
        CexioInterface.__init__(self, key, secret, db_interface, cexio_logger)
        self.ccy_books = {}

        self.actions_on_msg_map['tick'] = self.tick_act
        self.actions_on_msg_map['md_update'] = self.md_update_act
        self.actions_on_msg_map['order-book-subscribe'] = self.order_book_snapshot_act

        self.logger.debug("actions_in_msg_map for {} = {}".format(type(self), self.actions_on_msg_map.keys()))

    def tick_act(self):
        pass

    def md_update_act(self, msg):
        self.logger.info("[WS] md_update received")
        self.logger.info(msg['data'])


    def order_book_snapshot_act(self, msg):
        self.logger.info("[WS] order_book_snapshot received")
        #        self.ccy_books[]

        asks = msg['data']['asks']
        bids = msg['data']['bids']
        depth = min(len(asks), len(bids))
        order_book = {"bids": {}, "asks": {}}
        for i in range(depth):
            order_book["bids"][bids[i][0]] = bids[i][1]
            order_book["asks"][asks[i][0]] = asks[i][1]

        self.logger.debug("Full order_book:"
                          "\n bids = {}"
                          "\n asks = {}".format(order_book["bids"], order_book["asks"]))
        # self.logger.info("\n{}\n".format(tabulate(limits, headers=["BQty", "Bid", "Ask", "AQty"], tablefmt="simple", floatfmt=".6f")))
        self.display_order_book(order_book)

    def start_listening(self):
        self.start()
        time.sleep(1)


    def subscribe_tickers(self):
        self.logger.info("[WS] Subscribing to tickers")
        oid = str(self.get_timestamp()) + "_tickers"
        msg = json.dumps({
            "e": "subscribe",
            "rooms": ["tickers"]
        })
        self.ws.send(msg)

    def subscribe_orderbook(self, symbol1, symbol2, depth = -1):
        if not self.is_connected:
            self.start()
        self.logger.info("[WS] Subscribing to pair {}/{}".format(symbol1, symbol2))
        oid = "{}_orderbook_{}_{}".format(str(self.get_timestamp()), symbol1, symbol2)
        msg = json.dumps({
            "e": "order-book-subscribe",
            "data": {
                "pair": [
                    symbol1,
                    symbol2
                ],
                "subscribe": True,
                "depth": depth
            },
            "oid": oid
        })
        self.ws.send(msg)


    def sort_order_book(self, order_book, depth = 10):
        bids = order_book['bids'].keys()
        asks = order_book['asks'].keys()
        bids.sort(reverse=True)
        asks.sort()
        if depth == 0:
            depth = min(depth, len(bids), len(asks))

        return [(order_book['bids'].get(bids[i]), bids[i], asks[i], order_book['asks'].get(asks[i])) for i in range(depth)]

    def display_order_book(self, order_book):

        self.logger.info("\n{}\n".format(tabulate(self.sort_order_book(order_book), headers=["BQty", "Bid", "Ask", "AQty"], tablefmt="simple", floatfmt=".6f")))

    def smooth_bid_ask(self, depth):
        pass





class CexioTraderBot(CexioInterface):
    """
    CexioTraderBot allows trading
    """
    def __init__(self, key, secret, cexio_logger):
        CexioInterface.__init__(self, key, secret, cexio_logger)

    def update_balance(self):
        self.logger.info("Requesting updated balance...")
        oid = str(self.get_timestamp()) + "_get-balance"
        msg = json.dumps({
                "e": "get-balance",
                "data": {},
                "oid": oid
                })
        self.ws.send(msg)