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
from db_interface import CrateDbInterface

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
#       type: (String, String, CrateDbInterface, Logger) -> object
        self.key = key
        self.secret = secret
        self.logger = cexio_logger
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

    def restart_ws(self):
        self.logger.info("Restarting WebSocket")
        thread.start_new_thread(self.ws.run_forever, ())
        time.sleep(1)

    def on_message(self, ws, message):
        msg = yaml.load(message)
        self.logger.debug("[WS] on_message event, msg = {}".format(msg))
        if msg['e'] in self.actions_on_msg_map.keys():
            self.logger.debug("Message {} recognised, launching {}".format(msg, self.actions_on_msg_map[msg['e']]))
            self.actions_on_msg_map[msg['e']](msg)
        else:
            self.logger.warning("Unknown message: {}, will be discarded".format(msg))

    def connected_act(self, msg):
        self.is_connected = True
        self.logger.info("[WS] Websocket connected")

    def pong_act(self, msg):
        self.logger.debug("Sending pong")
        self.ws.send(json.dumps({
            "e": "pong"
        }))

    def disconnecting_act(self, msg):
        self.is_connected = False
        self.logger.info("[WS] Disconnecting...")

    def auth_act(self, msg):
        self.logger.info("[WS] Authenticated to exchange")

    def on_error(self, ws, error):
        #type: (websocket.WebSocketApp, str)
        self.logger.info("[WS] on_error event, err = {}".format(error))
        self.logger.debug("ws = {}".format(ws))
        self.logger.debug("[WS] Re-connecting")
        self.ws.send(self.auth_request())

    def on_close(self, ws, message):
        self.logger.info("[WS] on_close event, msg = {}".format(message))
        self.logger.warning("Websocket closed")

    def on_open(self, ws):
        def run(*args):
            self.connect()
        thread.start_new_thread(run, ())

    def get_timestamp(self):
        return int(time.time())

    def get_ms_timestamp(self):
        return int(time.time()*1000)

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
        self.ccy_order_books = {}
        #ccy_order_books_buffer: dict(ccy) -> {timestamp: [(bid_qty, bid, ask, ask_qty)]}
        self.ccy_order_books_buffer = {}
        self.number_of_updates = 0
        self.actions_on_msg_map['tick'] = self.tick_act
        self.actions_on_msg_map['md_update'] = self.md_update_act
        self.actions_on_msg_map['order-book-subscribe'] = self.order_book_snapshot_act

        self.logger.debug("actions_in_msg_map for {} = {}".format(type(self), self.actions_on_msg_map.keys()))

    def tick_act(self, msg):
        pass

    def order_book_snapshot_act(self, msg):
        self.logger.info("[WS] order_book_snapshot received")
        ccy = msg['oid'][-6:]
        self.ccy_order_books[ccy] = self.build_order_book(msg['data'])
        self.buffer_order_book(ccy)
        self.logger.debug("Orderbook for {} is now: \n{}".format(ccy, self.ccy_order_books[ccy]))
        self.record_best_bid_ask(ccy)
        self.number_of_updates += 1

    def md_update_act(self, msg):
        self.logger.debug("[WS] md_update received")
        ccy = str(msg['data']['pair']).replace(":", "")
        # commenting to test ws disconnection
        #self.update_order_book(ccy, msg['data']['bids'], msg['data']['asks'])
        self.record_best_bid_ask(ccy)
        self.record_order_book_histo(ccy)
        self.number_of_updates += 1
        self.logger.debug("{}th orderbook_snapshot".format(self.number_of_updates))

    def record_best_bid_ask(self, ccy):
        data_dict = {
            'timestamp': self.get_ms_timestamp(),
            'ccy_id': ccy,
            'ask_qty': self.ccy_order_books[ccy]['asks'].values()[0],
            'ask': self.ccy_order_books[ccy]['asks'].keys()[0],
            'bid_qty': self.ccy_order_books[ccy]['bids'].values()[0],
            'bid': self.ccy_order_books[ccy]['bids'].keys()[0]
        }
        query = self.db.insert_query('raw_market_data_histo', data_dict)
        self.db.run_query(query)

    def record_order_book_histo(self, ccy):
        self.logger.debug("Recording order_book in order_book_histo")
        data_dict = {
            'timestamp': self.get_ms_timestamp(),
            'ccy_id': ccy,
            'order_book': self.ccy_order_books[ccy]
        }
        query = self.db.insert_query('order_book_histo', data_dict)
        self.db.run_query(query)

    #TODO: to finish
    def buffer_order_book(self, ccy):
        self.logger.debug("Buffering orderbook")
        self.ccy_order_books_buffer[ccy] = {self.get_ms_timestamp(), self.in_depth_limit(ccy, self.ccy_order_books[ccy])}


    def update_order_book(self, ccy, bids, asks):
        self.logger.debug("Updating order_book for {}".format(ccy))
        self.logger.debug("Order book was : {}".format(self.ccy_order_books[ccy]))
        for b in bids:
            if b[1] == 0:
                self.ccy_order_books[ccy]['bids'].pop(b[0])
            else:
                self.ccy_order_books[ccy]['bids'][b[0]] = b[1]
        for a in asks:
            if a[1] == 0:
                self.ccy_order_books[ccy]['asks'].pop(a[0])
            else:
                self.ccy_order_books[ccy]['asks'][a[0]] = a[1]

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
        self.logger.info("[WS] Subscribing to pair {}/{}".format(symbol1, symbol2))
        if not self.is_connected:
            self.start()
            self.logger.warning("Not connected, trying to reconnect")
            time.sleep(2)
        oid = "{}_orderbook_{}{}".format(str(self.get_timestamp()), symbol1, symbol2)
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

    def build_order_book(self, data, order_book = {'bids': OrderedDict(), 'asks': OrderedDict()}):
        self.logger.debug("building order book for {}".format(data['pair']))
        for b in data['bids']:
            order_book['bids'][b[0]] = b[1]
        for a in data['asks']:
            order_book['asks'][a[0]] = a[1]
        return order_book



    def in_depth_limit(self, ccy, order_book):
        #type: (str, {'bids': {},'asks': {}}) -> (float, float, float, float)
        bid_qty = 0
        bid = 0
        ask = 0
        ask_qty = 0
        self.logger.debug("Calculating in depth limit for {}".format(ccy))
        count_b = count_a = 0
        for b, bq in order_book['bids'].iteritems():
            bid_qty += bq
            bid += b
        bid = bid/count_b
        for a, aq in order_book['asks'].iteritems():
            ask_qty += aq
            ask += a
        ask = ask/count_a
        return (bid_qty, bid, ask, ask_qty)

class CexioTraderBot(CexioInterface):
    """
    CexioTraderBot allows trading
    """
    def __init__(self, key, secret, db_interface, cexio_logger):
        CexioInterface.__init__(self, key, secret, db_interface, cexio_logger)
        self.start()

    def update_balance(self):
        self.logger.info("Requesting updated balance...")
        oid = str(self.get_timestamp()) + "_get-balance"
        msg = json.dumps({
                "e": "get-balance",
                "data": {},
                "oid": oid
                })
        self.ws.send(msg)

    """def order_balance(self):
        self.logger.info("Requesting order balance...")
        oid = str(self.get_timestamp()) + "_get-obalance"
        msg = json.dumps({
            "e": "obalance",
            "data": {},
            "oid": oid
        })
        self.ws.send(msg)"""
