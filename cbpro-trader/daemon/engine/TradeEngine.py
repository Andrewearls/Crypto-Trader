import time
import logging
import threading
import datetime
import itertools
import math
from decimal import Decimal, ROUND_DOWN
from .Product import Product


class TradeEngine:
    def __init__(self, auth_client, mongo_connection, product_list=['BTC-USD', 'ETH-USD', 'LTC-USD'], fiat='USD', is_live=False, max_slippage=Decimal('0.10')):
        self.logger = logging.getLogger('trader-logger')
        self.error_logger = logging.getLogger('error-logger')
        self.mc = mongo_connection
        self.auth_client = auth_client
        self.product_list = product_list
        self.fiat_currency = fiat
        self.is_live = is_live
        self.market_orders = True  # TODO: make this a config option
        self.available_products = []
        self.products = []
        self.balances = {}
        self.stop_update_order_thread = False
        self.last_order_update = time.time()
        self.all_open_orders = []
        self.recent_fills = []
        for product in self.product_list:
            self.products.append(Product(auth_client, product_id=product))
        self.last_balance_update = 0
        self.update_amounts()
        self.init_available_products()
        self.last_balance_update = time.time()
        self.max_slippage = max_slippage
        self.update_order_thread = threading.Thread(target=self.update_orders, name='update_orders')
        self.update_order_thread.start()

    def close(self, exit=False):
        if exit:
            self.stop_update_order_thread = True
        for product in self.products:
            # Setting both flags will close any open order threads
            product.buy_flag = False
            product.sell_flag = False
            # Cancel any orders that may still be remaining
            product.order_in_progress = False
        try:
            self.auth_client.cancel_all()
        except Exception:
            self.error_logger.exception(datetime.datetime.now())

    def get_product_by_product_id(self, product_id='BTC-USD'):
        for product in self.products:
            if product.product_id == product_id:
                return product
        return None

    def init_available_products(self):
        for product in self.auth_client.get_products():
            self.available_products.append(product.get('id'))

    def update_orders(self):
        while not self.stop_update_order_thread:
            need_updating = False
            for product in self.products:
                if product.order_in_progress:
                    need_updating = True

            if time.time() - self.last_order_update >= 1.0:
                self.temp_recent_fills = []
                try:
                    for product in self.products:
                        self.temp_recent_fills += list(itertools.islice(self.auth_client.get_fills(product_id=product.product_id), 5))
                        self.recent_fills = sorted(self.temp_recent_fills, key=lambda x: x['created_at'], reverse=True)[:5]
                except Exception:
                    self.error_logger.exception(datetime.datetime.now())
                if need_updating:
                    try:
                        self.all_open_orders = list(self.auth_client.get_orders())
                        for product in self.products:
                            product.open_orders = []
                        for order in self.all_open_orders:
                            self.get_product_by_product_id(order.get('product_id')).open_orders.append(order)
                    except Exception:
                        self.error_logger.exception(datetime.datetime.now())
                elif not need_updating:
                    self.all_open_orders = []
                self.last_order_update = time.time()
            time.sleep(0.01)

    def round_fiat(self, money):
        return Decimal(money).quantize(Decimal('.01'), rounding=ROUND_DOWN)

    def round_coin(self, money):
        # Coin base records to the ten-quadrillionth
        # For now round down to the hundred-millionth place value
        return Decimal(money).quantize(Decimal('.00000001'), rounding=ROUND_DOWN)

    def update_amounts(self):
        # If more than one second since last update
        if time.time() - self.last_balance_update > 1.0:
            try:
                self.last_balance_update = time.time()
                ret = self.auth_client.get_accounts()
                if isinstance(ret, list):
                    # For each type of currency held
                    for account in ret:
                        # Record the balance rounded down
                        self.balances[account['currency']] = self.round_coin(account.get('available'))

                self.mc.fills_log(self.recent_fills)
                # self.logger.debug("logging fills")

            except Exception:
                self.error_logger.exception(datetime.datetime.now())
                return
            self.balances['fiat_equivalent'] = Decimal('0.0')
            # For each product being traded
            for product in self.products:
                if not product.meta and product.order_book.get_current_ticker() and product.order_book.get_current_ticker().get('price'):
                    # Add the projected cost of any held currencies
                    self.balances['fiat_equivalent'] += self.get_base_currency_from_product_id(product.product_id, update=False) * Decimal(product.order_book.get_current_ticker().get('price'))
            # Then add any reserved fiat
            self.balances['fiat_equivalent'] += self.balances[self.fiat_currency]

    def print_amounts(self):
        self.logger.debug("[BALANCES] %s: %.2f BTC: %.8f" % (self.fiat_currency, self.balances[self.fiat_currency], self.balances['BTC']))

    def place_buy(self, product=None, partial='1.0'):
        amount = self.get_quoted_currency_from_product_id(product.product_id) * Decimal(partial)
        bid = product.order_book.get_ask() - Decimal(product.quote_increment)
        amount = self.round_coin(Decimal(amount) / Decimal(bid))

        if amount < Decimal(product.min_size):
            amount = self.get_quoted_currency_from_product_id(product.product_id)
            bid = product.order_book.get_ask() - Decimal(product.quote_increment)
            amount = self.round_coin(Decimal(amount) / Decimal(bid))

        if amount >= Decimal(product.min_size):
            self.logger.debug("Placing buy... Price: %.8f Size: %.8f" % (bid, amount))
            ret = self.auth_client.place_limit_order(product.product_id, "buy", size=str(amount),
                                                     price=str(bid), post_only=True)
            if ret.get('status') == 'pending' or ret.get('status') == 'open':
                product.open_orders.append(ret)
            return ret
        else:
            ret = {'status': 'done'}
            return ret

    def buy(self, product=None, amount=None):
        product.order_in_progress = True
        last_order_update = 0
        starting_price = product.order_book.get_ask() - Decimal(product.quote_increment)
        try:
            ret = self.place_buy(product=product, partial='0.5')
            bid = ret.get('price')
            amount = self.get_quoted_currency_from_product_id(product.product_id)
            while product.buy_flag and (amount >= Decimal(product.min_size) or len(product.open_orders) > 0):
                if (((product.order_book.get_ask() - Decimal(product.quote_increment)) / starting_price) - Decimal('1.0')) * Decimal('100.0') > self.max_slippage:
                    self.auth_client.cancel_all(product_id=product.product_id)
                    self.auth_client.place_market_order(product.product_id, "buy", funds=str(self.get_quoted_currency_from_product_id(product.product_id)))
                    product.order_in_progress = False
                    return
                if ret.get('status') == 'rejected' or ret.get('status') == 'done' or ret.get('message') == 'NotFound':
                    ret = self.place_buy(product=product, partial='0.5')
                    bid = ret.get('price')
                elif not bid or Decimal(bid) < product.order_book.get_ask() - Decimal(product.quote_increment):
                    if len(product.open_orders) > 0:
                        ret = self.place_buy(product=product, partial='1.0')
                    else:
                        ret = self.place_buy(product=product, partial='0.5')
                    for order in product.open_orders:
                        if order.get('id') != ret.get('id'):
                            self.auth_client.cancel_order(order.get('id'))
                    bid = ret.get('price')
                if ret.get('id') and time.time() - last_order_update >= 1.0:
                    try:
                        ret = self.auth_client.get_order(ret.get('id'))
                        last_order_update = time.time()
                    except ValueError:
                        self.error_logger.exception(datetime.datetime.now())
                        pass
                amount = self.get_quoted_currency_from_product_id(product.product_id)
                time.sleep(0.01)
            self.auth_client.cancel_all(product_id=product.product_id)
            amount = self.get_quoted_currency_from_product_id(product.product_id)
        except Exception:
            product.order_in_progress = False
            self.error_logger.exception(datetime.datetime.now())
        self.auth_client.cancel_all(product_id=product.product_id)
        product.order_in_progress = False

    def place_sell(self, product=None, partial='1.0'):
        amount = self.round_coin(self.get_base_currency_from_product_id(product.product_id) * Decimal(partial))
        if amount < Decimal(product.min_size):
            amount = self.get_base_currency_from_product_id(product.product_id)
        ask = product.order_book.get_bid() + Decimal(product.quote_increment)

        if amount >= Decimal(product.min_size):
            self.logger.debug("Placing sell... Price: %.2f Size: %.8f" % (ask, amount))
            ret = self.auth_client.place_limit_order(product.product_id, "sell", size=str(amount),
                                                     price=str(ask), post_only=True)
            if ret.get('status') == 'pending' or ret.get('status') == 'open':
                product.open_orders.append(ret)
            return ret
        else:
            ret = {'status': 'done'}
            return ret

    def sell(self, product=None, amount=None):
        product.order_in_progress = True
        last_order_update = 0
        starting_price = product.order_book.get_bid() + Decimal(product.quote_increment)
        try:
            ret = self.place_sell(product=product, partial='0.5')
            ask = ret.get('price')
            amount = self.get_base_currency_from_product_id(product.product_id)
            while product.sell_flag and (amount >= Decimal(product.min_size) or len(product.open_orders) > 0):
                if (Decimal('1') - ((product.order_book.get_bid() + Decimal(product.quote_increment)) / starting_price)) * Decimal('100.0') > self.max_slippage:
                    self.auth_client.cancel_all(product_id=product.product_id)
                    self.auth_client.place_market_order(product.product_id, "sell", size=str(self.get_base_currency_from_product_id(product.product_id)))
                    product.order_in_progress = False
                    return
                if ret.get('status') == 'rejected' or ret.get('status') == 'done' or ret.get('message') == 'NotFound':
                    ret = self.place_sell(product=product, partial='0.5')
                    ask = ret.get('price')
                elif not ask or Decimal(ask) > product.order_book.get_bid() + Decimal(product.quote_increment):
                    if len(product.open_orders) > 0:
                        ret = self.place_sell(product=product, partial='1.0')
                    else:
                        ret = self.place_sell(product=product, partial='0.5')
                    for order in product.open_orders:
                        if order.get('id') != ret.get('id'):
                            self.auth_client.cancel_order(order.get('id'))
                    ask = ret.get('price')
                if ret.get('id') and time.time() - last_order_update >= 1.0:
                    try:
                        ret = self.auth_client.get_order(ret.get('id'))
                    except ValueError:
                        self.error_logger.exception(datetime.datetime.now())
                        pass
                    last_order_update = time.time()
                amount = self.get_base_currency_from_product_id(product.product_id)
                time.sleep(0.01)
            self.auth_client.cancel_all(product_id=product.product_id)
            amount = self.get_base_currency_from_product_id(product.product_id)
        except Exception:
            product.order_in_progress = False
            self.error_logger.exception(datetime.datetime.now())
        self.auth_client.cancel_all(product_id=product.product_id)
        product.order_in_progress = False

    def get_base_currency_from_product_id(self, product_id, update=True):
        if update:
            self.update_amounts()
        return self.balances[product_id[:3]]

    def get_quoted_currency_from_product_id(self, product_id):
        self.update_amounts()
        return self.balances[product_id[4:]]

    def determine_trades(self, product_id, period_list, indicators):
        # Get current values of held instruments
        self.update_amounts()

        # if trades can be made
        if self.is_live:
            product = self.get_product_by_product_id(product_id)

            new_buy_flag = True
            new_sell_flag = False
            for cur_period in period_list:
                # Moving Average Strategy
                sma_trend_positive = Decimal(indicators[cur_period.name]['sma_trend']) > Decimal('0.0')
                sma_trend_negative = Decimal(indicators[cur_period.name]['sma_trend']) < Decimal('0.0')

                current_price = indicators[cur_period.name]['close']
                projected_market_bottom = indicators[cur_period.name]['bband_lower_1']

                below_market_bottom = current_price < projected_market_bottom
                above_market_bottom = current_price >= projected_market_bottom

                market_rising = sma_trend_positive and above_market_bottom
                market_falling = sma_trend_negative and below_market_bottom

                last_purchase_price = float(self.recent_fills[-1]["price"])
                incurring_losses = current_price < last_purchase_price
                emergency_sell = below_market_bottom and incurring_losses

                new_buy_flag = new_buy_flag and above_market_bottom
                new_sell_flag = new_sell_flag or emergency_sell

                # High Low Prediction Strategy

                # Calculate the BEP for buying at this price

                # If the BEP is below the bband upper band
                bep = math.ceil(indicators[cur_period.name]['bep'](self.balances[self.fiat_currency]))
                profit_expected = bep < math.ceil(indicators[cur_period.name]['bband_upper_1'])

                new_buy_flag = new_buy_flag and profit_expected

                # If product is >= Last purchase BEP
                cur_period_balance = float(self.balances[cur_period.name])
                cur_period_price = cur_period_balance * float(indicators[cur_period.name]['close'])

                sell_point = indicators['sell_point'](self.recent_fills)
                cur_period_profiting = current_price > sell_point

                new_sell_flag = new_sell_flag or cur_period_profiting

                # Don't sell if we would buy
                new_sell_flag = new_sell_flag and not new_buy_flag

                self.mc.indicator_log(indicators[cur_period.name], new_buy_flag, new_sell_flag, sell_point=sell_point)

            # if product_id == 'LTC-BTC' or product_id == 'ETH-BTC':
            #     ltc_or_eth_fiat_product = self.get_product_by_product_id(product_id[:3] + '-' + self.fiat_currency)
            #     btc_fiat_product = self.get_product_by_product_id('BTC-' + self.fiat_currency)
            #     new_buy_flag = new_buy_flag and ltc_or_eth_fiat_product.buy_flag
            #     new_sell_flag = new_sell_flag and btc_fiat_product.buy_flag

            if new_buy_flag:
                self.mc.placing_buy()
                if product.sell_flag:
                    product.last_signal_switch = time.time()
                product.sell_flag = False
                product.buy_flag = True
                amount = self.round_fiat(self.get_quoted_currency_from_product_id(product_id))
                if amount >= Decimal(product.min_size):
                    if self.market_orders:
                        ret = self.auth_client.place_market_order(product.product_id, "buy", funds=str(amount))
                        self.logger.debug(ret)
                        self.logger.debug(amount)
                    else:
                        if not product.order_in_progress:
                            bid = product.order_book.get_ask() - Decimal(product.quote_increment)
                            amount = self.round_coin(Decimal(amount) / Decimal(bid))
                            product.order_thread = threading.Thread(target=self.buy, name='buy_thread', kwargs={'product': product})
                            product.order_thread.start()
            elif new_sell_flag:
                self.mc.placing_sell()
                if product.buy_flag:
                    product.last_signal_switch = time.time()
                product.buy_flag = False
                product.sell_flag = True
                amount_of_coin = self.round_coin(self.get_base_currency_from_product_id(product_id))
                if amount_of_coin >= Decimal(product.min_size):
                    if self.market_orders:
                        self.auth_client.place_market_order(product.product_id, "sell", size=str(amount_of_coin))
                    else:
                        if not product.order_in_progress:
                            product.order_thread = threading.Thread(target=self.sell, name='sell_thread', kwargs={'product': product})
                            product.order_thread.start()
            else:
                product.buy_flag = False
                product.sell_flag = False
