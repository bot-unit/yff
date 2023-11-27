# -*- coding: UTF-8 -*-
"""
    Unit Test Lab
    2023-11-26
    Description:
    
"""

import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)


class YahooFinanceParser:

    @staticmethod
    def _parse_spark_meta(data) -> dict:
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
        error = top_level.get('error')
        if error is not None:
            raise ValueError(error)
        charts = []
        metas = {}
        for result in top_level.get('result'):
            try:
                meta = YahooFinanceParser._parse_spark_meta(result['response'][0]['meta'])
                timestamps = result['response'][0]['timestamp']
                indicators = result['response'][0]['indicators']['quote'][0]
                df_chart = pd.DataFrame(indicators, index=timestamps)
                df_chart.index = pd.to_datetime(df_chart.index, unit='s')
                df_chart.rename(columns={df_chart.columns[0]: meta['symbol']}, inplace=True)
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
    def parse_chart(data):
        pass

    @staticmethod
    def parse_downloads(data):
        pass

    @staticmethod
    def parse_options(data):
        pass

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
