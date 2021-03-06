import talib
import logging
import numpy as np
from decimal import Decimal


class IndicatorSubsystem:
    def __init__(self, period_list, mongo_connection):
        self.logger = logging.getLogger('trader-logger')
        self.mc = mongo_connection
        self.current_indicators = {}
        self.calculate_sell_point()
        self.period_list = period_list
        for period in self.period_list:
            self.current_indicators[period.name] = {}

    def recalculate_indicators(self, cur_period):
        total_periods = len(cur_period.candlesticks)
        if total_periods > 0:
            closing_prices = cur_period.get_closing_prices()
            closing_prices_close = np.append(closing_prices, cur_period.cur_candlestick.close)
            self.highs = np.append(cur_period.get_highs(), cur_period.cur_candlestick.high)
            self.lows = np.append(cur_period.get_lows(), cur_period.cur_candlestick.low)
            volumes = np.append(cur_period.get_volumes(), cur_period.cur_candlestick.volume)

            self.calculate_sma(cur_period.name, closing_prices_close)
            self.calculate_vol_macd(cur_period.name, volumes)
            self.calculate_avg_volume(cur_period.name, volumes)
            # self.calculate_stoch(cur_period.name, closing_prices)
            self.calculate_stochrsi(cur_period.name, closing_prices)

            self.current_indicators[cur_period.name]['close'] = cur_period.cur_candlestick.close
            self.current_indicators[cur_period.name]['total_periods'] = total_periods

            self.calculate_bbands(cur_period.name, closing_prices_close)
            self.calculate_bep(cur_period.name, closing_prices_close)


    def calculate_sma(self, period_name, closing_prices):
        sma = talib.SMA(closing_prices, timeperiod=9)

        self.current_indicators[period_name]['sma'] = sma[-1]
        self.current_indicators[period_name]['sma_trend'] = sma[-1] - sma[-2]

    def calculate_adx(self, period_name, close):
        adx = talib.ADX(self.highs, self.lows, close, timeperiod=14)

        self.current_indicators[period_name]['adx'] = adx[-1]

    # Bollinger Bands
    # Calculate the estimated price range
    # Should cover 95% of the price range
    # Not sure why there are two or how they are calculated
    def calculate_bbands(self, period_name, close):
        timeperiod = 20
        upperband_1, middleband_1, lowerband_1 = talib.BBANDS(close, timeperiod=timeperiod, nbdevup=1, nbdevdn=1, matype=0)

        self.current_indicators[period_name]['bband_upper_1'] = upperband_1[-1]
        self.current_indicators[period_name]['bband_lower_1'] = lowerband_1[-1]

        upperband_2, middleband_2, lowerband_2 = talib.BBANDS(close, timeperiod=timeperiod, nbdevup=2, nbdevdn=2, matype=0)

        self.current_indicators[period_name]['bband_upper_2'] = upperband_2[-1]
        self.current_indicators[period_name]['bband_lower_2'] = lowerband_2[-1]

    def calculate_macd(self, period_name, closing_prices):
        macd, macd_sig, macd_hist = talib.MACD(closing_prices, fastperiod=12,
                                               slowperiod=26, signalperiod=9)
        self.current_indicators[period_name]['macd'] = macd[-1]
        self.current_indicators[period_name]['macd_sig'] = macd_sig[-1]
        self.current_indicators[period_name]['macd_hist'] = macd_hist[-1]
        self.current_indicators[period_name]['macd_hist_diff'] = Decimal(macd_hist[-1]) - Decimal(macd_hist[-2])

    def calculate_vol_macd(self, period_name, volumes):
        macd, macd_sig, macd_hist = talib.MACD(volumes, fastperiod=50,
                                               slowperiod=200, signalperiod=14)
        self.current_indicators[period_name]['vol_macd'] = macd[-1]
        self.current_indicators[period_name]['vol_macd_sig'] = macd_sig[-1]
        self.current_indicators[period_name]['vol_macd_hist'] = macd_hist[-1]

    def calculate_avg_volume(self, period_name, volumes):
        avg_vol = talib.SMA(volumes, timeperiod=15)

        self.current_indicators[period_name]['avg_volume'] = avg_vol[-1]

    def calculate_obv(self, period_name, closing_prices, volumes):
        obv = talib.OBV(closing_prices, volumes)
        obv_ema = talib.EMA(obv, timeperiod=3)

        self.current_indicators[period_name]['obv_ema'] = obv_ema[-1]
        self.current_indicators[period_name]['obv'] = obv[-1]

    def calculate_sar(self, period_name, highs, lows):
        sar = talib.SAR(highs, lows)

        self.current_indicators[period_name]['sar'] = sar[-1]

    def calculate_stochrsi(self, period_name, closing_prices):
        fastk, fastd = talib.STOCHRSI(closing_prices, timeperiod=14, fastk_period=3, fastd_period=3, fastd_matype=0)
        self.current_indicators[period_name]['stochrsi_fastk'] = fastk[-1]
        self.current_indicators[period_name]['stochrsi_fastd'] = fastd[-1]

    def calculate_stoch(self, period_name, closing_prices):
        slowk, slowd = talib.STOCH(self.highs, self.lows, closing_prices, fastk_period=14, slowk_period=2, slowk_matype=0, slowd_period=3, slowd_matype=0)
        self.current_indicators[period_name]['stoch_slowk'] = slowk[-1]
        self.current_indicators[period_name]['stoch_slowd'] = slowd[-1]

    def calculate_mfi(self, period_name, highs, lows, closing_prices, volumes):
        mfi = talib.MFI(highs, lows, closing_prices, volumes)

        self.current_indicators[period_name]['mfi'] = mfi[-1]

    def calculate_bep(self, period_name, closing_prices):
        def calculate(fiat_balance):
            # This Works Don't Change it!
            fiat_balance = float(fiat_balance)
            # goal_profit = fiat_balance / .995
            fiat_minus_fees = fiat_balance * .995
            coin_amount = fiat_minus_fees / float(closing_prices[-1])
            # trading_fee = fiat_balance * .005
            return fiat_balance / (coin_amount * .995)

        self.current_indicators[period_name]['bep'] = calculate

    def calculate_sell_point(self):
        # This works now don't change
        def calculate(last_trades):
            cost = 0
            profit_margin = .995
            coin_amount = 0
            for trade in last_trades:
                if trade['side'] == 'buy':
                    # self.logger.debug(trade)
                    volume = float(trade["usd_volume"])
                    coin_amount += float(trade["size"])
                    taker_fee = float(trade["fee"])
                    cost += volume + taker_fee
                else:
                    break

            try:
                sell_point = cost / (coin_amount * profit_margin)
            except ZeroDivisionError:
                sell_point = 0
            return sell_point

        self.current_indicators['sell_point'] = calculate
