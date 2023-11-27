# -*- coding: UTF-8 -*-
"""
    Unit Test Lab
    2023-11-25
    Description:
    
"""

import asyncio
from yff import YahooFinanceFetcher, YahooFinanceParser
from yff.constants import VALID_MODULES


async def main():
    yahoo = YahooFinanceFetcher.instance()
    parser = YahooFinanceParser()
    period1 = 1700077200
    period2 = 1701009736
    async with yahoo:
        await yahoo.load_yahoo_cookies('yahoo.cookies')
        # await yahoo.warm_up()
        # response = await yahoo.get_spark(['AAPL', 'MSFT'])
        # df = parser.parse_spark(response)
        # print(df)
        # print(df.meta)
        # response = await yahoo.get_quote_type(['BTC-USD', 'MSFT'])
        # df = parser.parse_quote_type(response)
        # print(df)
        # response = await yahoo.search('apple')
        # quotes, news = parser.parse_search(response)
        # print(quotes)
        # print(news)
        # response = await yahoo.get_recommendations(['AAPL', 'BA', 'META'])
        # print(parser.parse_recommendations(response))
        # response = await yahoo.get_options('QQQ')
        # options, expirations, quote = parser.parse_options(response)
        # print(options)
        # print(options.calls)
        # print(options.puts)
        # print(expirations)
        # print(quote)
        # date=1701734400
        # period1 = 1391986800
        # period2 = 1701101212
        # response = await yahoo.get_chart('AAPL', start=period1, end=period2, interval='1d')
        # df = parser.parse_chart(response)
        # print(df.meta)
        # print(df.head())
        # print(df.dividends)
        # print(df.splits)
        response = await yahoo.download_chart('DBK.DE')
        df = parser.parse_downloads(response)
        print(df)
        # print(await yahoo.get_quote('DBK.DE'))
        # print("summary")
        #for m in VALID_MODULES:
            #print(m, await yahoo.get_quote_summary('AAPL', modules=[m]))
        await yahoo.save_yahoo_cookies('yahoo.cookies')


if __name__ == '__main__':
    YahooFinanceFetcher()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
    pass


