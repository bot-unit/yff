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
        #response = await yahoo.get_spark(['AAPL', 'MSFT'])
        #df = parser.parse_spark(response)
        #print(df.meta)
        #response = await yahoo.get_quote_type(['BTC-USD', 'MSFT'])
        #df = parser.parse_quote_type(response)
        #print(df)
        #response = await yahoo.search('apple')
        #quotes, news = parser.parse_search(response)
        #print(quotes)
        #print(news)
        #response = await yahoo.get_recommendations(['AAPL', 'BA', 'META'])
        #print(parser.parse_recommendations(response))
        print(await yahoo.get_options('AAPL'))
        # print(await yahoo.get_chart('AAPL'))
        # print(await yahoo.download_chart('DBK.DE'))
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


