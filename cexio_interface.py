import hmac
import hashlib
import datetime, time, sys
import json, yaml
import pdb
import logging
import thread

import websocket

web_socket_url = 'wss://ws.cex.io/ws/'

class CexioInterface(object):

    """
    Generic Cexio Interface class.
    Child classes:
        CexioMarketDataHandler
        CexioTraderBot
    """
    
    def __init__(self, key, secret, cexio_logger):
        self.key = key
        self.secret = secret
        self.logger = cexio_logger
        #Initialising connection to websocketi
        self.balance = None
        self.actions_on_msg_map = {
            "connected": self.connected_act,
            "ping": self.pong_act,
            "disconnecting": self.disconnecting_act,
            "auth": self.auth_act
            }
    

    def on_message(self, ws, message):
        msg = yaml.load(message)
        self.logger.info("[WS] on_message event, msg = {}".format(msg))
        if msg['e'] in self.actions_on_msg_map.keys():
            self.logger.debug("msg e: {} is in map: {}".format(msg['e'], self.actions_on_msg_map.keys()))
            self.actions_on_msg_map[msg['e']]()
        else:
            self.logger.warning("Unknown message: {}, will be discarded".format(msg))

    def connected_act(self):
        pass

    def pong_act(self):
		self.logger.debug("Sending pong")
		self.ws.send(json.dumps({
			"e": "pong"
		}))

    def disconnecting_act(self):
        pass

    def auth_act(self):
        pass

    def on_error(self, ws, error):
        self.logger.info("[WS] on_error event, err = {}".format(error))

    def on_close(self, ws):
        self.logger.info("[WS] on_message event, msg = {}".format(message))
        self.logger.info("Websocket closed")

    def on_open(self, ws):
        self.logger.debug("##### on_open event code here #####")
        #self.connect()
        def run(*args):
            self.connect()
            """
            self.logger.info("Opening connexion to: " + web_socket_url)
            self.ws = create_connection(web_socket_url)
            self.logger.debug("Handshake: " + self.ws.recv())
            """
        thread.start_new_thread(run, ())


    def start(self):
        self.logger.info("Starting new interface")
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(web_socket_url,
                                  on_message = self.on_message,
                                  on_error = self.on_error,
                                  on_close = self.on_close)
        self.ws.on_open = self.on_open
        thread.start_new_thread(self.ws.run_forever, ())

    def get_timestamp(self):
        return int(time.time())

    def create_signature(self):  # (string key, string secret) 
        timestamp = self.get_timestamp()  # UNIX timestamp in seconds
        nonce = "{}{}".format(timestamp, self.key)
        self.logger.info("Creating signature with key = {} and nonce = {}".format(self.key, nonce))
        return timestamp, hmac.new(self.secret, nonce, hashlib.sha256).hexdigest()

    def auth_request(self):
        timestamp, signature = self.create_signature()

        return json.dumps({'e': 'auth',
                'auth': {'key': self.key, 'signature': signature, 'timestamp': timestamp,}, 'oid': 'auth', })

    def connect(self):
        self.logger.info("Connecting to websocket: " + web_socket_url)
        self.ws.send(self.auth_request())

        #self.logger.debug("Connection result: " + self.ws.recv())




class CexioMarketDataHandler(CexioInterface):
    def __init__(self, key, secret, cexio_logger):
        print "init CexioMarketDataHandler"
        CexioInterface.__init__(self, key, secret, cexio_logger)
        self.actions_on_msg_map['tick'] = self.tick_act
        self.logger.debug("actions_on_msg_map = {}".format(self.actions_on_msg_map))


    def subscribe_ticker(self):
        self.logger.info("Subscribing to tickers")
        oid = str(self.get_timestamp()) + "_subscribe"
        msg = json.dumps({
            "e": "subscribe",
            #"rooms": ["BTC", "ETH", "USD"]
            "rooms": ["tickers"]
        })
        self.ws.send(msg)
        self.logger.info("Subscription request send: {}".format(msg))

    def tick_act(self):
        pass


class CexioTraderBot(CexioInterface):
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
