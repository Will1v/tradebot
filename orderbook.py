from collections import OrderedDict
import time


class Orderbook(object):

    def __init__(self, ccy, depth, logger):
        self.bids = dict()
        self.asks = dict()
        self.ccy = ccy
        self.depth = depth
        self.update_nb = 0
        self.init_time = time.time()
        self.logger = logger

    def __str__(self):
        if len(self.bids) == 0 or len(self.asks) == 0:
            out = "Orderbook for {} is empty!".format(self.ccy)
        else:
            try:
                zipmap = map(None, sorted(self.bids.keys(), reverse=True), sorted(self.asks.keys()))
                ll = ["\t{}\t{}\t{}\t{}".format(self.bids.get(l[0]), l[0], l[1], self.asks.get(l[1])) for l in zipmap]
                out = "\n{}".format(l for l in ll)
            except Exception as e:
                out = "Exception: {}".format(e)
            out += "\nccy: {}\tdepth: {}\tinit_time: {}\t update_nb: {}".format(self.ccy, self.depth, self.init_time, self.update_nb)
        return out


    def build(self, data):
        bid_len = len(data['bids'])
        ask_len = len(data['asks'])
        for bid, bidq in data['bids']:
            self.bids[bid] = bidq
        for ask, askq in data['asks']:
            self.asks[ask] = askq

    def get_sorted(self):
        sorted_bids = OrderedDict([(bid, self.bids[bid]) for bid in sorted(self.bids.keys(), reverse=True)])
        sorted_asks = OrderedDict([(ask, self.asks[ask]) for ask in sorted(self.asks.keys())])
        return {'bids': sorted_bids, 'asks': sorted_asks}

    def is_valid(self):
        return False if len(self.bids) == 0 or len(self.asks) == 0 or self.is_crossed() else True

    def is_crossed(self):
        return False if max(self.bids.keys()) < min(self.asks.keys()) else True

    def update(self,data):
        self.logger.debug("[Orderbook] Updating orderbook {} for update message id: {}".format(self.ccy, data['id']))
        # updating bids
        for bid, bidq in data['bids']:
            if int(bidq) == 0:
                self.logger.debug("Popping {}@{}".format(bidq, bid))
                try:
                    self.bids.pop(bid)
                except:
                    self.logger.warning("[Orderbook] Trouble while popping bid! (update id: {})".format(data['id']))
            else:
                self.logger.debug("Adding {}@{}".format(bidq, bid))
                self.bids[bid] = bidq

        # updating asks
        for ask, askq in data['asks']:
            if int(askq) == 0:
                self.logger.debug("Popping {}@{}".format(askq, ask))
                try:
                    self.asks.pop(ask)
                except:
                    self.logger.warning("[Orderbook] Trouble while popping bid! (update id: {})".format(data['id']))
            else:
                self.logger.debug("Adding {}@{}".format(askq, ask))
                self.asks[ask] = askq





