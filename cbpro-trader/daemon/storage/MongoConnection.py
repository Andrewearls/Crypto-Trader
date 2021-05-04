from pymongo import MongoClient
from datetime import datetime
from time import gmtime, strftime
from pytz import timezone
import pytz
import logging


class MongoConnection(object):
    def __init__(self, mongo_url):
        self.connection = MongoClient(mongo_url)
        self.db = self.connection.trading_bot
        self.logger = logging.getLogger('trader-logger')
        self.error_logger = logging.getLogger('error-logger')

        self.last_indicator_entry = self.get_last_indicator_entry
        self.last_fills_entry = {}

    def get_time(self):
        date_format = '%m/%d/%Y %H:%M:%S %Z'
        date = datetime.now(tz=pytz.utc)
        date = date.astimezone(timezone('US/Pacific'))
        return date.strftime(date_format)

    def get_last_indicator_entry(self):
        # needs work
        data = {}
        result = self.db.indicator_log.find().limit(1).sort([('_id', -1)])
        try:
            for indicator, value in result.items():
                if indicator not in ['_id', 'time']:
                    data[indicator] = value
        except AttributeError:
            self.error_logger.info(msg="No collections found, collection will need to be created.")

        self.logger.debug(f'retrieved {data}')

        return data

    def indicator_log(self, indicators, buy_flag, sell_flag, sell_point=0):
        # collection = self.db.indicator_log
        # persist indicators to database
        data = {'buy_flag': str(buy_flag), 'sell_flag': str(sell_flag), 'sell_point': sell_point}
        for indicator, value in indicators.items():
            try:
                float(value)
                data[indicator] = value
                # self.logger.debug(f"{indicator} {value}")
            except TypeError:
                if indicator == 'bep' and 'close' in indicators:
                    data[indicator] = float(value(indicators['close']))
                else:
                    continue

        if data != self.last_indicator_entry:
            # self.logger.debug(data)
            # self.logger.debug(self.last_indicator_entry)
            self.last_indicator_entry = data.copy()
            data['time'] = self.get_time()
            self.db.indicator_log.insert(data, manipulate=False)

    def fills_log(self, fills):
        # self.logger.debug(fills)
        # self.logger.debug("trying to log fills")
        data = {}
        for fill in fills:
            data[str(fill['trade_id'])] = fill
            # self.logger.debug(fill)

        if self.last_fills_entry != data:
            # self.logger.debug(self.last_fills_entry)
            self.last_fills_entry = data.copy()
            # self.logger.debug(data)

            data['time'] = self.get_time()
            self.db.fills_log.insert(data, manipulate=False)

        # collection = self.db.fill_log
        # collection.insert(fill)

    def placing_buy(self):
        data = self.last_indicator_entry.copy()
        data['time'] = self.get_time()
        self.db.placing_buy.insert(data, manipulate=False)

    def placing_sell(self):
        data = self.last_indicator_entry.copy()
        data['time'] = self.get_time()
        self.db.placing_sell.insert(data, manipulate=False)
