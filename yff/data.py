# -*- coding: UTF-8 -*-
"""
    Unit Test Lab
    2023-11-27
    Description:
    
"""

import time

import aiohttp

from yff.fetcher import YahooFinanceAsyncFetcher
from yff.parser import YahooFinanceParser as Parser


class YahooFinanceData:
    def __init__(self, raise_on_error: bool = False):
        self._yf = None
        self.raise_on_error = raise_on_error

    async def __aenter__(self):
        self._yf = YahooFinanceAsyncFetcher()
        await self._yf.create_session()
        await self._yf.warmup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._yf.close_session()
        self._yf = None

    async def _get_spark(self, symbol: str | list, range_: str, interval: str):
        if self._yf is None:
            async with YahooFinanceAsyncFetcher() as yf:
                return await yf.get_spark(symbol, range_, interval)
        return await self._yf.get_spark(symbol, range_, interval)

    async def get_spark(
        self,
        symbols: str | list,
        range_: str = "1d",
        interval: str = "5m",
        max_attempts: int = 3,
    ):
        for _ in range(max_attempts):
            try:
                response = await self._get_spark(symbols, range_, interval)
                return Parser.parse_spark(response)
            except aiohttp.ClientError as e:
                if self.raise_on_error:
                    raise e
                continue

    async def _get_quote_type(self, symbol: str | list):
        if self._yf is None:
            async with YahooFinanceAsyncFetcher() as yf:
                return await yf.get_quote_type(symbol)
        return await self._yf.get_quote_type(symbol)

    async def get_quote_type(self, symbols: str | list, max_attempts: int = 3):
        for _ in range(max_attempts):
            try:
                response = await self._get_quote_type(symbols)
                return Parser.parse_quote_type(response)
            except aiohttp.ClientError as e:
                if self.raise_on_error:
                    raise e
                continue

    async def _get_quote(self, symbol: str | list):
        if self._yf is None:
            async with YahooFinanceAsyncFetcher() as yf:
                return await yf.get_quote(symbol)
        return await self._yf.get_quote(symbol)

    async def get_quote(self, symbols: str | list, max_attempts: int = 3):
        for _ in range(max_attempts):
            try:
                response = await self._get_quote(symbols)
                return Parser.parse_quote(response)
            except aiohttp.ClientError as e:
                if self.raise_on_error:
                    raise e
                continue

    async def _get_quote_summary(self, symbol: str, modules: list):
        if self._yf is None:
            async with YahooFinanceAsyncFetcher() as yf:
                return await yf.get_quote_summary(symbol, modules)
        return await self._yf.get_quote_summary(symbol, modules)

    async def get_quote_summary(
        self, symbol: str, modules: list, max_attempts: int = 3
    ):
        for _ in range(max_attempts):
            try:
                response = await self._get_quote_summary(symbol, modules)
                return Parser.parse_quote_summary(response)
            except aiohttp.ClientError as e:
                if self.raise_on_error:
                    raise e
                continue

    async def _get_chart(
        self,
        symbol: str,
        start: int,
        end: int,
        interval: str,
        prepost: bool,
        adj_close: bool,
        events: list,
    ):
        if self._yf is None:
            async with YahooFinanceAsyncFetcher() as yf:
                return await yf.get_chart(
                    symbol, start, end, interval, prepost, adj_close, events
                )
        return await self._yf.get_chart(
            symbol, start, end, interval, prepost, adj_close, events
        )

    async def get_chart(
        self,
        symbol: str,
        start: int = 0,
        end: int = 0,
        interval: str = "1d",
        prepost: bool = False,
        adj_close: bool = True,
        events: list = None,
        max_attempts: int = 3,
    ):
        if end == 0:
            end = int(time.time())
        if start == 0 or start >= end:
            if interval in ("1d", "5d", "1wk", "1mo", "3mo"):
                start = 0
            else:
                start = end - 86400
        if (
            interval in ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h")
            and (end - start) > 86400
        ):
            start = end - 86400
        for _ in range(max_attempts):
            try:
                response = await self._get_chart(
                    symbol, start, end, interval, prepost, adj_close, events
                )
                return Parser.parse_chart(response)
            except aiohttp.ClientError as e:
                if self.raise_on_error:
                    raise e
                continue

    async def get_price_history(
        self,
        symbol: str,
        start: int = 0,
        end: int = 0,
        interval: str = "1d",
        prepost: bool = False,
        adj_close: bool = True,
        events: list = None,
        max_attempts: int = 3,
    ):
        return await self.get_chart(
            symbol, start, end, interval, prepost, adj_close, events, max_attempts
        )

    async def _download_chart(
        self, symbol: str, period1: int, period2: int, interval: str, adj_close: bool
    ):
        if self._yf is None:
            async with YahooFinanceAsyncFetcher() as yf:
                return await yf.download_chart(
                    symbol, period1, period2, interval, adj_close
                )
        return await self._yf.download_chart(
            symbol, period1, period2, interval, adj_close
        )

    async def download_chart(
        self,
        symbol: str,
        period1: int,
        period2: int,
        interval: str = "1d",
        adj_close: bool = True,
        max_attempts: int = 3,
    ):
        for _ in range(max_attempts):
            try:
                response = await self._download_chart(
                    symbol, period1, period2, interval, adj_close
                )
                return Parser.parse_downloads(response)
            except aiohttp.ClientError as e:
                if self.raise_on_error:
                    raise e
                continue

    async def _get_options(self, symbol: str, expiration: int):
        if self._yf is None:
            async with YahooFinanceAsyncFetcher() as yf:
                return await yf.get_options(symbol, expiration)
        return await self._yf.get_options(symbol, expiration)

    async def get_options_chain(self, symbol: str, expiration, max_attempts: int = 3):
        for _ in range(max_attempts):
            try:
                response = await self._get_options(symbol, expiration)
                options, _, _ = Parser.parse_options(response)
                return options
            except aiohttp.ClientError as e:
                if self.raise_on_error:
                    raise e
                continue

    async def get_options_expirations(self, symbol: str, max_attempts: int = 3):
        for _ in range(max_attempts):
            try:
                response = await self._get_options(symbol, 0)
                _, expiration_dates, _ = Parser.parse_options(response)
                return expiration_dates
            except aiohttp.ClientError as e:
                if self.raise_on_error:
                    raise e
                continue

    async def _get_recommendations(self, symbol: str | list):
        if self._yf is None:
            async with YahooFinanceAsyncFetcher() as yf:
                return await yf.get_recommendations(symbol)
        return await self._yf.get_recommendations(symbol)

    async def get_recommendations(self, symbols: str | list, max_attempts: int = 3):
        for _ in range(max_attempts):
            try:
                response = await self._get_recommendations(symbols)
                return Parser.parse_recommendations(response)
            except aiohttp.ClientError as e:
                if self.raise_on_error:
                    raise e
                continue

    async def _search(self, query: str):
        if self._yf is None:
            async with YahooFinanceAsyncFetcher() as yf:
                return await yf.search(query)
        return await self._yf.search(query)

    async def search_quote(self, query: str, max_attempts: int = 3):
        for _ in range(max_attempts):
            try:
                response = await self._search(query)
                quotes, _ = Parser.parse_search(response)
                return quotes
            except aiohttp.ClientError as e:
                if self.raise_on_error:
                    raise e
                continue

    async def search_news(self, query: str, max_attempts: int = 3):
        for _ in range(max_attempts):
            try:
                response = await self._search(query)
                _, news = Parser.parse_search(response)
                return news
            except aiohttp.ClientError as e:
                if self.raise_on_error:
                    raise e
                continue

    # todo: add exception handling if requested module is not exist in response
    async def get_quote_info(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol,
            ["summaryProfile", "summaryDetail", "quoteType"],
            max_attempts=max_attempts,
        )
        answer = {}
        answer.update(response["summaryProfile"])
        answer.update(response["summaryDetail"])
        answer.update(response["quoteType"])
        return answer

    async def get_options_info(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["price", "summaryDetail"], max_attempts=max_attempts
        )
        answer = {}
        answer.update(response["price"])
        answer.update(response["summaryDetail"])
        return answer

    async def get_summary_profile(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["summaryProfile"], max_attempts=max_attempts
        )
        return response["summaryProfile"]

    async def get_summary_detail(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["summaryDetail"], max_attempts=max_attempts
        )
        return response["summaryDetail"]

    async def get_asset_profile(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["assetProfile"], max_attempts=max_attempts
        )
        return response["assetProfile"]

    async def get_fund_profile(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["fundProfile"], max_attempts=max_attempts
        )
        return response["fundProfile"]

    async def get_price(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["price"], max_attempts=max_attempts
        )
        return response["price"]

    async def get_esg_scores(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["esgScores"], max_attempts=max_attempts
        )
        return response["esgScores"]

    async def get_income_statement_history(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["incomeStatementHistory"], max_attempts=max_attempts
        )
        return response["incomeStatementHistory"]

    async def get_income_statement_history_quarterly(
        self, symbol: str, max_attempts: int = 3
    ):
        response = await self.get_quote_summary(
            symbol, ["incomeStatementHistoryQuarterly"], max_attempts=max_attempts
        )
        return response["incomeStatementHistoryQuarterly"]

    async def get_balance_sheet_history(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["balanceSheetHistory"], max_attempts=max_attempts
        )
        return response["balanceSheetHistory"]

    async def get_balance_sheet_history_quarterly(
        self, symbol: str, max_attempts: int = 3
    ):
        response = await self.get_quote_summary(
            symbol, ["balanceSheetHistoryQuarterly"], max_attempts=max_attempts
        )
        return response["balanceSheetHistoryQuarterly"]

    async def get_cash_flow_statement_history(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["cashFlowStatementHistory"], max_attempts=max_attempts
        )
        return response["cashFlowStatementHistory"]

    async def get_cash_flow_statement_history_quarterly(
        self, symbol: str, max_attempts: int = 3
    ):
        response = await self.get_quote_summary(
            symbol, ["cashFlowStatementHistoryQuarterly"], max_attempts=max_attempts
        )
        return response["cashFlowStatementHistoryQuarterly"]

    async def get_default_key_statistics(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["defaultKeyStatistics"], max_attempts=max_attempts
        )
        return response["defaultKeyStatistics"]

    async def get_financial_data(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["financialData"], max_attempts=max_attempts
        )
        return response["financialData"]

    async def get_calendar_events(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["calendarEvents"], max_attempts=max_attempts
        )
        return response["calendarEvents"]

    async def get_sec_filings(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["secFilings"], max_attempts=max_attempts
        )
        return response["secFilings"]

    async def get_upgrade_downgrade_history(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["upgradeDowngradeHistory"], max_attempts=max_attempts
        )
        return response["upgradeDowngradeHistory"]

    async def get_institution_ownership(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["institutionOwnership"], max_attempts=max_attempts
        )
        return response["institutionOwnership"]

    async def get_fund_ownership(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["fundOwnership"], max_attempts=max_attempts
        )
        return response["fundOwnership"]

    async def get_major_direct_holders(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["majorDirectHolders"], max_attempts=max_attempts
        )
        return response["majorDirectHolders"]

    async def get_major_holders_breakdown(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["majorHoldersBreakdown"], max_attempts=max_attempts
        )
        return response["majorHoldersBreakdown"]

    async def get_insider_transactions(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["insiderTransactions"], max_attempts=max_attempts
        )
        return response["insiderTransactions"]

    async def get_insider_holders(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["insiderHolders"], max_attempts=max_attempts
        )
        return response["insiderHolders"]

    async def get_net_share_purchase_activity(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["netSharePurchaseActivity"], max_attempts=max_attempts
        )
        return response["netSharePurchaseActivity"]

    async def get_earnings(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["earnings"], max_attempts=max_attempts
        )
        return response["earnings"]

    async def get_earnings_history(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["earningsHistory"], max_attempts=max_attempts
        )
        return response["earningsHistory"]

    async def get_earnings_trend(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["earningsTrend"], max_attempts=max_attempts
        )
        return response["earningsTrend"]

    async def get_industry_trend(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["industryTrend"], max_attempts=max_attempts
        )
        return response["industryTrend"]

    async def get_index_trend(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["indexTrend"], max_attempts=max_attempts
        )
        return response["indexTrend"]

    async def get_sector_trend(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["sectorTrend"], max_attempts=max_attempts
        )
        return response["sectorTrend"]

    async def get_recommendation_trend(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["recommendationTrend"], max_attempts=max_attempts
        )
        return response["recommendationTrend"]

    async def get_futures_chain(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["futuresChain"], max_attempts=max_attempts
        )
        futures = response["futuresChain"]
        return getattr(futures, "futures", [])

    async def get_futures_chain_details(self, symbol: str, max_attempts: int = 3):
        response = await self.get_quote_summary(
            symbol, ["futuresChain"], max_attempts=max_attempts
        )
        return response["futuresChain"]
