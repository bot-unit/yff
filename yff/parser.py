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
        top_level = data.get('quoteResponse')
        if top_level is None:
            raise ValueError('quoteResponse not found in data')
        quotas = {}
        for result in top_level.get('result'):
            try:
                quotas[result['symbol']] = result
            except KeyError:
                continue
        return quotas

    @staticmethod
    def _parse_raws(data: list) -> list:
        for item in data:
            try:
                del item['maxAge']
            except KeyError:
                pass
            for key, value in item.items():
                if isinstance(value, dict):
                    item[key] = value.get('raw', 0)
        return data

    @staticmethod
    def _parse_asset_profile(data):
        officers = YahooFinanceParser._parse_raws(data.get('companyOfficers', []))
        data['companyOfficers'] = officers
        return data

    @staticmethod
    def _parse_recommendation_trend(data):
        return data.get('trend', [])

    @staticmethod
    def _parse_cashflow_statement_history(data):
        data = data.get('cashflowStatements', [])
        data = YahooFinanceParser._parse_raws(data)
        try:
            cashflow = pd.DataFrame(data)
            cashflow.set_index('endDate', inplace=True)
            cashflow.index = pd.to_datetime(cashflow.index, unit='s')
        except ValueError:
            cashflow = pd.DataFrame()
        return cashflow

    @staticmethod
    def _parse_income_statement_history(data):
        data = data.get('incomeStatementHistory', [])
        data = YahooFinanceParser._parse_raws(data)
        try:
            income = pd.DataFrame(data)
            income.set_index('endDate', inplace=True)
            income.index = pd.to_datetime(income.index, unit='s')
        except ValueError:
            income = pd.DataFrame()
        return income

    @staticmethod
    def _parse_fund_ownership(data):
        data = data.get('ownershipList', [])
        data = YahooFinanceParser._parse_raws(data)
        return data

    @staticmethod
    def _parse_insider_holders(data):
        data = data.get('holders', [])
        data = YahooFinanceParser._parse_raws(data)
        return data

    @staticmethod
    def _parse_calendar_events(data):
        del data['maxAge']
        earnings = data.get('earnings', {})
        del data['earnings']
        data.update(earnings)
        return data

    @staticmethod
    def _parse_upgrade_downgrade_history(data):
        data = data.get('history', [])
        df = pd.DataFrame(data)
        df.set_index('epochGradeDate', inplace=True)
        df.index = pd.to_datetime(df.index, unit='s')
        return df

    @staticmethod
    def _parse_balance_sheet_history(data):
        data = data.get('balanceSheetStatements', [])
        data = YahooFinanceParser._parse_raws(data)
        try:
            balance = pd.DataFrame(data)
            balance.set_index('endDate', inplace=True)
            balance.index = pd.to_datetime(balance.index, unit='s')
        except ValueError:
            balance = pd.DataFrame()
        return balance

    @staticmethod
    def _parse_earnings_trend(data):
        trends = []
        for t in data.get('trend', []):
            try:
                trend = dict()
                trend['period'] = t['period']
                trend['endDate'] = t['endDate']
                trend['growth'] = t['growth']['raw']
                trend['earningsEstimateAvg'] = t['earningsEstimate']['avg'].get('raw', 0)
                trend['earningsEstimateLow'] = t['earningsEstimate']['low'].get('raw', 0)
                trend['earningsEstimateHigh'] = t['earningsEstimate']['high'].get('raw', 0)
                trend['earningsEstimateYearAgoEps'] = t['earningsEstimate']['yearAgoEps'].get('raw', 0)
                trend['earningsEstimateGrowth'] = t['earningsEstimate']['growth'].get('raw', 0)
                trend['revenueEstimateAvg'] = t['revenueEstimate']['avg'].get('raw', 0)
                trend['revenueEstimateLow'] = t['revenueEstimate']['low'].get('raw', 0)
                trend['revenueEstimateHigh'] = t['revenueEstimate']['high'].get('raw', 0)
                trend['epsTrendCurrent'] = t['epsTrend']['current'].get('raw', 0)
                trend['epsTrend7daysAgo'] = t['epsTrend']['7daysAgo'].get('raw', 0)
                trend['epsTrend30daysAgo'] = t['epsTrend']['30daysAgo'].get('raw', 0)
                trend['epsTrend60daysAgo'] = t['epsTrend']['60daysAgo'].get('raw', 0)
                trend['epsTrend90daysAgo'] = t['epsTrend']['90daysAgo'].get('raw', 0)
                trend['epsRevisionsUpLast7days'] = t['epsRevisions']['upLast7days'].get('raw', 0)
                trend['epsRevisionsUpLast30days'] = t['epsRevisions']['upLast30days'].get('raw', 0)
                trend['epsRevisionsDownLast30days'] = t['epsRevisions']['downLast30days'].get('raw', 0)
                trends.append(trend)
            except KeyError:
                continue
        df = pd.DataFrame(trends)
        df.set_index('endDate', inplace=True)
        df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
        return df

    @staticmethod
    def _parse_sec_filings(data):
        return data.get('filings', [])

    @staticmethod
    def _parse_institution_ownership(data):
        data = data.get('ownershipList', [])
        data = YahooFinanceParser._parse_raws(data)
        return data

    @staticmethod
    def _parse_earnings_history(data):
        data = data.get('history', [])
        data = YahooFinanceParser._parse_raws(data)
        df = pd.DataFrame(data)
        df.set_index('period', inplace=True)
        df['quarter'] = pd.to_datetime(df['quarter'], unit='s')
        return df

    @staticmethod
    def _parse_major_direct_holders(data):
        data = data.get('holders', [])
        data = YahooFinanceParser._parse_raws(data)
        return data

    @staticmethod
    def _parse_insider_transactions(data):
        data = data.get('transactions', [])
        data = YahooFinanceParser._parse_raws(data)
        df = pd.DataFrame(data)
        df.set_index('startDate', inplace=True)
        df.index = pd.to_datetime(df.index, unit='s')
        return df

    _quote_summary_parsers = {
        "summaryProfile": lambda data: data,  # not need to parse
        "summaryDetail": lambda data: data,  # not need to parse
        "assetProfile": lambda data: YahooFinanceParser._parse_asset_profile(data),
        "fundProfile": lambda data: data,  # no data, not need to parse
        "price": lambda data: data,  # not need to parse
        "quoteType": lambda data: data,  # not need to parse
        "esgScores": lambda data: data,  # not need to parse
        "incomeStatementHistory": lambda data: YahooFinanceParser._parse_income_statement_history(data),
        "incomeStatementHistoryQuarterly": lambda data: YahooFinanceParser._parse_income_statement_history(data),
        "balanceSheetHistory": lambda data: YahooFinanceParser._parse_balance_sheet_history(data),
        "balanceSheetHistoryQuarterly": lambda data: YahooFinanceParser._parse_balance_sheet_history(data),
        "cashFlowStatementHistory": lambda data: YahooFinanceParser._parse_cashflow_statement_history(data),
        "cashFlowStatementHistoryQuarterly": lambda data: YahooFinanceParser._parse_cashflow_statement_history(data),
        "defaultKeyStatistics": lambda data: data,  # not need to parse
        "financialData": lambda data: data,  # not need to parse
        "calendarEvents": lambda data: YahooFinanceParser._parse_calendar_events(data),
        "secFilings": lambda data: YahooFinanceParser._parse_sec_filings(data),
        "upgradeDowngradeHistory": lambda data: YahooFinanceParser._parse_upgrade_downgrade_history(data),
        "institutionOwnership": lambda data: YahooFinanceParser._parse_institution_ownership(data),
        "fundOwnership": lambda data: YahooFinanceParser._parse_fund_ownership(data),
        "majorDirectHolders": lambda data: YahooFinanceParser._parse_major_direct_holders(data),
        "majorHoldersBreakdown": lambda data: data,  # not need to parse
        "insiderTransactions": lambda data: YahooFinanceParser._parse_insider_transactions(data),
        "insiderHolders": lambda data: YahooFinanceParser._parse_insider_holders(data),
        "netSharePurchaseActivity": lambda data: data,  # not need to parse
        "earnings": lambda data: data,  # not need to parse
        "earningsHistory": lambda data: YahooFinanceParser._parse_earnings_history(data),
        "earningsTrend": lambda data: YahooFinanceParser._parse_earnings_trend(data),
        "industryTrend": lambda data: data,  # not need to parse
        "indexTrend": lambda data: data,  # not need to parse
        "sectorTrend": lambda data: data,  # not need to parse
        "recommendationTrend": lambda data: YahooFinanceParser._parse_recommendation_trend(data),
        "futuresChain": lambda data: data,  # need more info for parsing
    }

    @staticmethod
    def parse_quote_summary(data):
        top_level = data.get('quoteSummary')
        if top_level is None:
            raise ValueError('quoteSummary not found in data')
        result = top_level['result'][0]
        parsed = dict()
        for key, value in result.items():
            if key not in YahooFinanceParser._quote_summary_parsers:
                parsed[key] = value
            else:
                parsed[key] = YahooFinanceParser._quote_summary_parsers[key](value)
        return parsed

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
