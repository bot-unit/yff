# -*- coding: UTF-8 -*-
"""
    Unit Test Lab
    2023-11-27
    Description:
    
"""

from yff.fetcher import YahooFinanceFetcher


class YahooFinanceData:
    def __init__(self):
        self._yf = YahooFinanceFetcher()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
