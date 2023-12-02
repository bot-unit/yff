# -*- coding: UTF-8 -*-
"""
    Unit Test Lab
    2023-11-25
    Description:
    
"""

ENDPOINTS = {
    "crumb": "/v1/test/getcrumb",  # crumb for requests
    "spark": "/v7/finance/spark",  # quick info with close price data
    "quote_type": "/v1/finance/quoteType/",  # type of quote
    "quote": "/v7/finance/quote",  # quote current data
    "quote_summary": "/v10/finance/quoteSummary/",  # expanded quote data, depends on modules in query
    "chart": "/v8/finance/chart/",  # price data
    "download": "/v7/finance/download/",  # download price data
    "options": "/v7/finance/options/",  # options data
    "search": "/v1/finance/search",  # search quotes by text
    "recommendations": "/v6/finance/recommendationsbysymbol/",  # recommendations
}

VALID_RANGES = ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
VALID_INTERVALS = (
    "1m",
    "2m",
    "5m",
    "15m",
    "30m",
    "60m",
    "90m",
    "1h",
    "1d",
    "5d",
    "1wk",
    "1mo",
    "3mo",
)
VALID_PERIODS = ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
VALID_EVENTS = ("div", "split", "earn", "capitalGain")  # | sepperated
VALID_FILTERS = ("history", "dividend", "split")
QUOTE_TYPES = (
    "EQUITY",
    "ETF",
    "MUTUALFUND",
    "MONEYMARKET",
    "OPTION",
    "INDEX",
    "CRYPTOCURRENCY",
    "CURRENCY",
    "WARRANT",
    "BOND",
)
VALID_MODULES = (
    "summaryProfile",  # contains general information about the company
    "summaryDetail",  # prices + volume + market cap + etc
    "assetProfile",  # summaryProfile + company officers
    "fundProfile",  # ? No fundamentals data found for any of the summaryTypes=fundProfile
    "price",  # current prices
    "quoteType",  # quoteType
    "esgScores",  # Environmental, social, and governance (ESG) scores, sustainability and ethical performance of companies
    "incomeStatementHistory",
    "incomeStatementHistoryQuarterly",
    "balanceSheetHistory",
    "balanceSheetHistoryQuarterly",
    "cashFlowStatementHistory",
    "cashFlowStatementHistoryQuarterly",
    "defaultKeyStatistics",  # KPIs (PE, enterprise value, EPS, EBITA, and more)
    "financialData",  # Financial KPIs (revenue, gross margins, operating cash flow, free cash flow, and more)
    "calendarEvents",  # future earnings date
    "secFilings",  # SEC filings, such as 10K and 10Q reports
    "upgradeDowngradeHistory",  # upgrades and downgrades that analysts have given a company's stock
    "institutionOwnership",  # institutional ownership, holders and shares outstanding
    "fundOwnership",  # mutual fund ownership, holders and shares outstanding
    "majorDirectHolders",
    "majorHoldersBreakdown",
    "insiderTransactions",  # insider transactions, such as the number of shares bought and sold by company executives
    "insiderHolders",  # insider holders, such as the number of shares held by company executives
    "netSharePurchaseActivity",  # net share purchase activity, such as the number of shares bought and sold by company executives
    "earnings",  # earnings history
    "earningsHistory",
    "earningsTrend",  # earnings trend
    "industryTrend",
    "indexTrend",
    "sectorTrend",
    "recommendationTrend",
)
DEFAULT_MODULES = (
    "summaryProfile",
    "summaryDetail",
    "quoteType",
)
NON_STOCK_MODULES = (
    "price",
    "summaryDetail",
    "futuresChain",
)
