# -*- coding: UTF-8 -*-
"""
    Unit Test Lab
    2023-11-26
    Description:
    
"""

import pandas as pd
from io import StringIO
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)


class YahooFinanceParser:

    @staticmethod
    def _parse_meta(data) -> dict:
        # not all data needed
        parsed = dict()
        parsed['symbol'] = data['symbol']
        parsed['currency'] = data.get('currency')
        parsed['exchangeName'] = data.get('exchangeName')
        parsed['instrumentType'] = data.get('instrumentType')
        parsed['regularMarketPrice'] = data.get('regularMarketPrice')
        parsed['chartPreviousClose'] = data.get('chartPreviousClose')
        parsed['previousClose'] = data.get('previousClose')
        parsed['scale'] = data.get('scale')
        parsed['priceHint'] = data.get('priceHint')
        return parsed

    @staticmethod
    def parse_spark(data) -> pd.DataFrame:
        top_level = data.get('spark')
        if top_level is None:
            raise ValueError('spark not found in data')
        charts = []
        metas = {}
        for result in top_level.get('result'):
            try:
                meta = YahooFinanceParser._parse_meta(result['response'][0]['meta'])
                timestamps = result['response'][0]['timestamp']
                indicators = result['response'][0]['indicators']['quote'][0]['close']
                df_chart = pd.DataFrame({meta['symbol']: indicators}, index=timestamps)
                df_chart.index = pd.to_datetime(df_chart.index, unit='s')
                charts.append(df_chart)
                metas[meta['symbol']] = meta
            except KeyError:
                continue
        df = pd.concat(charts, axis=1)
        df.meta = metas  # setattr(df, 'meta', metas)
        return df

    @staticmethod
    def parse_quote_type(data) -> dict:
        top_level = data.get('quoteType')
        if top_level is None:
            raise ValueError('quoteType not found in data')
        quotas = {}
        for result in top_level.get('result'):
            try:
                quotas[result['symbol']] = result
            except KeyError:
                continue
        return quotas

    @staticmethod
    def parse_quote(data):
        pass

    @staticmethod
    def parse_quote_summary(data):
        pass

    @staticmethod
    def _parse_dividends(data: dict) -> pd.DataFrame:
        dividends = pd.DataFrame(data.values())
        dividends.set_index('date', inplace=True)
        dividends.index = pd.to_datetime(dividends.index, unit='s')
        return dividends

    @staticmethod
    def _parse_splits(data: dict) -> pd.DataFrame:
        splits = pd.DataFrame(data.values())
        splits.set_index('date', inplace=True)
        splits.index = pd.to_datetime(splits.index, unit='s')
        return splits

    @staticmethod
    def parse_chart(data):
        top_level = data.get('chart')
        if top_level is None:
            raise ValueError('chart not found in data')
        try:
            meta = YahooFinanceParser._parse_meta(top_level['result'][0]['meta'])
            timestamps = top_level['result'][0]['timestamp']
            indicators = top_level['result'][0]['indicators']['quote'][0]
            opens = indicators['open']
            closes = indicators['close']
            highs = indicators['high']
            lows = indicators['low']
            volumes = indicators['volume']
            adj = None
            if 'adjclose' in top_level['result'][0]['indicators']:
                adj = top_level['result'][0]['indicators']['adjclose'][0]['adjclose']
            if adj is not None:
                df = pd.DataFrame({'Open': opens, 'Close': closes, 'High': highs, 'Low': lows, 'Volume': volumes, 'Adj.Close': adj}, index=timestamps)
            else:
                df = pd.DataFrame({'Open': opens, 'Close': closes, 'High': highs, 'Low': lows, 'Volume': volumes}, index=timestamps)
            df.index = pd.to_datetime(df.index, unit='s')
            df.meta = meta
            if 'events' in top_level['result'][0]:
                events = top_level['result'][0]['events']
                if 'dividends' in events:
                    df.dividends = YahooFinanceParser._parse_dividends(events['dividends'])
                if 'splits' in events:
                    df.splits = YahooFinanceParser._parse_splits(events['splits'])
            return df
        except KeyError:
            pass
        return pd.DataFrame()

    @staticmethod
    def parse_downloads(data):
        try:
            df = pd.read_csv(StringIO(data))
            df.set_index('Date', inplace=True)
            df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
            return df
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    @staticmethod
    def parse_options(data) -> tuple[pd.DataFrame, list, dict]:
        top_level = data.get('optionChain')
        if top_level is None:
            raise ValueError('optionChain not found in data')
        expiration_dates = []
        quote = {}
        calls = pd.DataFrame()
        puts = pd.DataFrame()
        try:
            quote = top_level['result'][0]['quote']
            expiration_dates = top_level['result'][0]['expirationDates']
            calls = pd.DataFrame(top_level['result'][0]['options'][0]['calls'])
            calls.set_index('strike', inplace=True, drop=False)
            puts = pd.DataFrame(top_level['result'][0]['options'][0]['puts'])
            puts.set_index('strike', inplace=True, drop=False)
        except KeyError:
            pass
        d = dict()
        d['calls'] = calls
        d['puts'] = puts
        options = pd.concat(d, axis=1)
        # add to pd as attributes ?
        return options, expiration_dates, quote

    @staticmethod
    def _parse_search_quotes(data) -> dict:
        quotas = {}
        for q in data:
            try:
                quotas[q['symbol']] = q
            except KeyError:
                continue
        return quotas

    @staticmethod
    def _parse_search_news(data) -> list:
        news_list = []
        for n in data:
            news = dict()
            news['title'] = n.get('title')
            news['link'] = n.get('link')
            news['publisher'] = n.get('publisher')
            news['publishTime'] = n.get('providerPublishTime')
            news['type'] = n.get('type')
            try:
                news['image'] = n['thumbnail']['resolutions'][0]['url']
            except KeyError:
                news['image'] = None
            news_list.append(news)
        return news_list

    @staticmethod
    def parse_search(data) -> tuple[dict, list]:
        quotes = YahooFinanceParser._parse_search_quotes(data.get('quotes', []))
        news = YahooFinanceParser._parse_search_news(data.get('news', []))
        return quotes, news

    @staticmethod
    def parse_recommendations(data) -> set:
        recommendation = []
        requested = []
        try:
            for result in data['finance']['result']:
                requested.append(result['symbol'])
                for symbol in result['recommendedSymbols']:
                    recommendation.append(symbol['symbol'])
        except KeyError:
            pass
        return set([symbol for symbol in recommendation if symbol not in requested])
