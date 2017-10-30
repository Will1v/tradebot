import hmac
import hashlib
import time
import json, yaml
import thread
from collections import OrderedDict
from db_interface import CrateDbInterface
import Queue
from orderbook import Orderbook

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
        self.msg_queue = Queue.Queue()

    def start(self):
        self.logger.info("Starting new {}".format(type(self)))
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(web_socket_url,
                                  on_message = self.on_message,
                                  on_error = self.on_error,
                                  on_close = self.on_close)
        self.ws.on_open = self.on_open
        thread.start_new_thread(self.ws.run_forever, ())
        thread.start_new_thread(self.start_msg_listener, ())
        time.sleep(1)


    def restart(self):
        self.logger.info("Launching new start ({})".format(type(self)))
        self.start()
        self.subscribe_orderbook('BTC', 'USD', 5)
        self.subscribe_orderbook('BTC', 'EUR', 5)
        self.subscribe_orderbook('BTC', 'GBP', 5)


    def start_msg_listener(self):
        while True:
            self.logger.debug("Msg listener: waiting for new message...")
            next_msg = self.msg_queue.get()
            start_action = time.time()
            self.logger.debug("Message listener: next msg: {}".format(next_msg))
            if next_msg['e'] in self.actions_on_msg_map.keys():
                self.logger.debug("Message {} recognised, launching {}".format(next_msg, self.actions_on_msg_map[next_msg['e']]))
                self.actions_on_msg_map[next_msg['e']](next_msg)
            else:
                self.logger.warning("Unknown message: {}, will be discarded".format(next_msg))
            end_action = time.time()
            self.logger.debug("Msg listener took {} to process msg: {}".format(end_action - start_action, next_msg))

    def restart_ws(self):
        self.logger.info("Restarting WebSocket")
        thread.start_new_thread(self.ws.run_forever, ())
        time.sleep(1)

    def on_message(self, ws, message):
        msg = yaml.load(message)
        #self.msg_queue.put(msg)
        self.logger.debug("[WS] on_message event, msg = {}".format(msg))
        start_time = time.time()
        if msg['e'] in self.actions_on_msg_map.keys():
            # ping can't wait, so it bypasses the message queue
            if msg['e'] == 'ping':
                self.pong_act(msg)
            else:
                self.logger.debug("Message {} recognised, launching {}".format(msg, self.actions_on_msg_map[msg['e']]))
                self.msg_queue.put(msg)
                #self.actions_on_msg_map[msg['e']](msg)
        else:
            self.logger.warning("Unknown message: {}, will be discarded".format(msg))
        end_time = time.time()
        self.logger.debug("on_message: Message stacked, handled or discarded, ready to accept new message (took {}s)".format(end_time - start_time))

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
        self.logger.warning("[WS] on_error event, err = {}".format(error))
        self.logger.debug("ws = {}".format(ws))
        self.logger.debug("Need to reconnect. Launching restart")
        self.is_connected = False
        self.restart()

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
        self.ccy_depth = {}
        #ccy_order_books_buffer: dict(ccy) -> {timestamp: [(bid_qty, bid, ask, ask_qty)]}
        self.ccy_order_books_buffer = {}
        self.actions_on_msg_map['tick'] = self.tick_act
        self.actions_on_msg_map['md_update'] = self.md_update_act
        self.actions_on_msg_map['order-book-subscribe'] = self.order_book_snapshot_act
        self.debug_number_of_updates = {}
        self.debug_init_time = {}
        self.logger.debug("actions_in_msg_map for {} = {}".format(type(self), self.actions_on_msg_map.keys()))

        #Methods you want to relaunch on reconnect.
        self.methods_to_relaunch_on_reconnect = []

    def tick_act(self, msg):
        # see: https://cex.io/websocket-api#ticker-subscription
        self.logger.warning("tick_act ! ####To Be Implemented###")
        pass

    def order_book_snapshot_act(self, msg):
        ccy = msg['data']['pair']
        self.logger.info("[WS] order_book_snapshot received for {}".format(ccy))
        self.ccy_order_books[ccy].build(msg['data'])
        self.logger.debug("Orderbook for {} is now: \n{}".format(ccy, self.ccy_order_books[ccy]))
        self.record_best_bid_ask(ccy)
        self.ccy_order_books[ccy].update_nb += 1

    def md_update_act(self, msg):
        self.logger.debug("[WS] md_update received")
        ccy = str(msg['data']['pair'])
        self.logger.debug("update for: {}".format(ccy))
        # commenting to test ws disconnection
        self.logger.debug("Orderbook before update:"
                          " \nbids {}: {}\nasks {}: {}".format(ccy, self.ccy_order_books[ccy].get_sorted()['bids'],
                                                         ccy, self.ccy_order_books[ccy].get_sorted()['asks']))
        self.ccy_order_books[ccy].update(msg['data'])
        self.record_best_bid_ask(ccy)
        self.logger.debug("Orderbook after update:"
                          " \nbids {}: {}\nasks {}: {}".format(ccy, self.ccy_order_books[ccy].get_sorted()['bids'],
                                                         ccy, self.ccy_order_books[ccy].get_sorted()['asks']))
        self.ccy_order_books[ccy].update_nb += 1
        self.logger.debug("orderbook update #{} for {} (average of {} updates/sec)".format(self.ccy_order_books[ccy].update_nb, ccy, self.ccy_order_books[ccy].update_nb/(time.time() - self.ccy_order_books[ccy].init_time)))
        if not self.ccy_order_books[ccy].is_valid():
            self.resubscribe_orderbook(ccy)

    """ TODO: needs to be refactored to new Orderbook objects"""
    def record_best_bid_ask(self, ccy):
        self.logger.debug("ccy = {}".format(ccy))
        try:
            data_dict = {
                'timestamp': self.get_ms_timestamp(),
                'ccy_id': ccy,
                'bid': self.ccy_order_books[ccy].get_sorted()['bids'].keys()[0],
                'bid_qty': self.ccy_order_books[ccy].get_sorted()['bids'].values()[0],
                'ask': self.ccy_order_books[ccy].get_sorted()['asks'].keys()[0],
                'ask_qty': self.ccy_order_books[ccy].get_sorted()['asks'].values()[0]
            }
            self.logger.debug("Orderbook sorted = \nbids: {}\nasks: {}".format(self.ccy_order_books[ccy].get_sorted()['bids'], self.ccy_order_books[ccy].get_sorted()['asks']))
            self.logger.debug("Datadict about to be insterted in raw_market_data_histo: {}".format(data_dict))
            query = self.db.insert_query('raw_market_data_histo', data_dict)
            self.db.run_query(query)
        except Exception as e:
            self.logger.debug("Couldn't record_best_bid_ask for {}".format(ccy))
            self.logger.debug("Exception thrown: \n{}".format(e))
            self.logger.debug("Orderbook was: \n{}".format(self.ccy_order_books[ccy]))



    def record_order_book_histo(self, ccy):
        self.logger.debug("Recording order_book in order_book_histo")
        data_dict = {
            'timestamp': self.get_ms_timestamp(),
            'ccy_id': ccy,
            'order_book': self.ccy_order_books[ccy]
        }
        query = self.db.insert_query('order_book_histo', data_dict)
        self.db.run_query(query)

    def start_listening(self):
        self.start()
        #time.sleep(1)


    def subscribe_tickers(self):
        self.logger.info("[WS] Subscribing to tickers")
        oid = str(self.get_timestamp()) + "_tickers"
        msg = json.dumps({
            "e": "subscribe",
            "rooms": ["tickers"]
        })
        self.ws.send(msg)

    def subscribe_orderbook(self, symbol1, symbol2, depth = 5):
        ccy = "{}:{}".format(symbol1, symbol2)
        self.ccy_order_books[ccy] = Orderbook(ccy, depth, self.logger)
        self.logger.info("[WS] Subscribing to pair {}".format(ccy))
        self.debug_init_time[ccy] = time.time()
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

    def resubscribe_orderbook(self, pair, depth = 5):
        self.unsubscribe_orderbook(pair)
        self.subscribe_orderbook(pair[:3], pair[4:], self.ccy_order_books[pair].depth)
        pass

    def unsubscribe_orderbook(self, pair):
        symbol1 = pair[:3]
        symbol2 = pair[4:]
        oid = "{}_orderbook-unsubscribe_{}{}".format(str(self.get_timestamp()), symbol1, symbol2)
        msg = json.dumps({
            "e": "order-book-unsubscribe",
            "data": {
                "pair": [
                    symbol1,
                    symbol2
                ]
            },
            "oid": oid
        })
        self.ws.send(msg)

    def build_order_book(self, data):
        order_book = {'bids': OrderedDict(), 'asks': OrderedDict()}
        self.logger.debug("[build_order_book] Initialising order book to: {}".format(order_book))
        self.logger.debug("[build_order_book] Building order book for {}".format(data['pair']))
        for b in data['bids']:
            order_book['bids'][b[0]] = b[1]
        for a in data['asks']:
            order_book['asks'][a[0]] = a[1]
        self.logger.debug("[build_order_book] returning: {}".format(order_book))
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
            count_b += 1
        bid = bid/count_b
        for a, aq in order_book['asks'].iteritems():
            ask_qty += aq
            ask += a
            count_a += 1
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
