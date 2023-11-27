# -*- coding: UTF-8 -*-
"""
    Unit Test Lab
    2023-11-27
    Description:
    
"""

from yff.fetcher import YahooFinanceAsyncFetcher


class YahooFinanceData:
    def __init__(self):
        self._yf = YahooFinanceAsyncFetcher()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
