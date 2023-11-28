# -*- coding: UTF-8 -*-
"""
    Unit Test Lab
    2023-11-25
    Description:
    
"""


import time
import aiohttp
import requests
import random
from bs4 import BeautifulSoup

from yff.utils import SingletonMeta
from yff.constants import *


USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ' \
             'AppleWebKit/537.36 (KHTML, like Gecko) ' \
             'Chrome/92.0.4515.159 Safari/537.36'

API_URLS = [
    "https://query1.finance.yahoo.com",
    "https://query2.finance.yahoo.com"
]


class YahooFinanceAsyncFetcher(metaclass=SingletonMeta):
    @classmethod
    def instance(cls):
        with cls._lock:
            if cls not in cls._instances:
                raise RuntimeError(f"{cls} is not initialized. Initialize first")
            return cls._instances[cls]

    def __init__(self, timeout: int = 60):
        self._session = None
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._crumb = ''

    def session(self):
        return self._session

    async def create_session(self):
        self._session = aiohttp.ClientSession(raise_for_status=True, timeout=self._timeout, headers={'User-Agent': USER_AGENT})

    async def close_session(self):
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def __aenter__(self):
        if self._session is None or self._session.closed:
            await self.create_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_session()

    async def _get(self, url: str, params: dict = None):
        if self._session is None or self._session.closed:
            await self.create_session()
        resp = await self._session.get(url, params=params, timeout=self._timeout)
        return resp

    async def _post(self, url: str, data: dict = None):
        if self._session is None or self._session.closed:
            await self.create_session()
        resp = await self._session.post(url, data=data, timeout=self._timeout)
        return resp

    async def _get_text(self, url: str, params: dict = None):
        try:
            resp = await self._get(url, params=params)
            return await resp.text()
        except aiohttp.ClientError as e:
            raise e

    async def _get_json(self, url: str, params: dict = None, with_crumb: bool = False, first_attempt: bool = True):
        if with_crumb:
            if params is None:
                params = dict()
            params['crumb'] = await self._get_crumb()
        try:
            resp = await self._get(url, params=params)
            return await resp.json()
        except aiohttp.ClientResponseError as e:
            if e.status == 401 and with_crumb and first_attempt:
                await self._update_crumb()
                return await self._get_json(url, params=params, with_crumb=with_crumb, first_attempt=False)
            else:
                raise e
        except aiohttp.ClientError as e:
            raise e

    def _build_url(self, path: str) -> str:
        return random.choice(API_URLS) + path

    async def _get_yahoo_cookies(self):
        if self._session is None or self._session.closed:
            await self.create_session()
        resp = await self._session.get("https://guce.yahoo.com/consent", timeout=self._timeout, allow_redirects=True)
        # todo: check non-EU countries steps
        # we can check resp.history for redirects
        if resp.url.host == 'consent.yahoo.com':
            # should accept cookies
            content = await resp.text()
            soup = BeautifulSoup(content, 'html.parser')
            csrfTokenInput = soup.find('input', attrs={'name': 'csrfToken'})
            csrfToken = csrfTokenInput['value']
            sessionIdInput = soup.find('input', attrs={'name': 'sessionId'})
            sessionId = sessionIdInput['value']
            originalDoneUrl = 'https://finance.yahoo.com/'
            namespace = 'yahoo'
            data = [
                ('agree', 'agree'),
                ('agree', 'agree'),
                ('consentUUID', 'default'),
                ('sessionId', sessionId),
                ('csrfToken', csrfToken),
                ('originalDoneUrl', originalDoneUrl),
                ('namespace', namespace),
            ]
            await self._session.post(f'https://consent.yahoo.com/v2/collectConsent?sessionId={sessionId}', data=data, allow_redirects=True)

    async def save_yahoo_cookies(self, path):
        cookie = self._session.cookie_jar
        cookie.save(path)

    async def load_yahoo_cookies(self, path: str):
        if self._session is None or self._session.closed:
            await self.create_session()
        try:
            self._session.cookie_jar.load(path)
        except FileNotFoundError:
            pass

    async def _update_crumb(self, first_attempt: bool = True):
        url = self._build_url(ENDPOINTS['crumb'])
        try:
            resp = await self._get_text(url)
            if resp == '':
                if first_attempt:
                    await self._get_yahoo_cookies()
                    await self._update_crumb(first_attempt=False)
                else:
                    raise RuntimeError('Failed to get crumb')
            else:
                self._crumb = resp
        except aiohttp.ClientResponseError as e:
            if e.status == 429:  # need cookies
                if first_attempt:
                    await self._get_yahoo_cookies()
                    return await self._update_crumb(first_attempt=False)
            raise e

    async def _get_crumb(self) -> str:
        if self._crumb == '':
            await self._update_crumb()
        return self._crumb

    async def warmup(self):
        await self._get_yahoo_cookies()
        await self._get_crumb()

    async def get_spark(self, symbols: str | list, range_: str = '1d', interval: str = '5m'):
        params = dict()
        params['symbols'] = ','.join(symbols) if isinstance(symbols, list) else symbols
        params['range'] = range_ if range_ in VALID_RANGES else '1d'
        params['interval'] = interval if interval in VALID_INTERVALS else '5m'
        params['formatted'] = 'false'
        url = self._build_url(ENDPOINTS['spark'])
        return await self._get_json(url, params=params)

    async def get_quote_type(self, symbols: str | list):
        params = dict()
        params['symbol'] = ','.join(symbols) if isinstance(symbols, list) else symbols
        url = self._build_url(ENDPOINTS['quote_type'])
        return await self._get_json(url, params=params)

    async def get_quote(self, symbols: str | list):  # requires crumb
        params = dict()
        params['symbols'] = ','.join(symbols) if isinstance(symbols, list) else symbols
        params['formatted'] = 'false'
        url = self._build_url(ENDPOINTS['quote'])
        return await self._get_json(url, params=params, with_crumb=True)

    async def get_quote_summary(self, symbol: str, modules: list = None):  # requires crumb
        params = dict()
        params['symbol'] = symbol
        if modules is not None and len(modules):
            params['modules'] = ','.join(modules)
        else:
            params['modules'] = ','.join(VALID_MODULES)
        params['formatted'] = 'false'
        url = self._build_url(ENDPOINTS['quote_summary'])
        return await self._get_json(url, params=params, with_crumb=True)

    async def get_non_stock_quote_summary(self, symbol: str):  # requires crumb
        params = dict()
        params['symbol'] = symbol
        params['modules'] = ','.join(NON_STOCK_MODULES)
        params['formatted'] = 'false'
        url = self._build_url(ENDPOINTS['quote_summary'])
        return await self._get_json(url, params=params, with_crumb=True)

    async def get_chart(self, symbol: str, start: int = 0, end: int = 0, interval: str = '5m', prepost: bool = False, adj_close: bool = True, events: list = None):
        params = dict()
        params['symbol'] = symbol
        if end == 0:
            end = int(time.time())
        if start >= end or start == 0:
            params['period1'] = end - 86400 * 7
            params['period2'] = end
        else:
            params['period1'] = start
            params['period2'] = end
        if events is not None:
            if len(events):
                params['events'] = '%7C'.join(events)  # | sepperated
        else:
            params['events'] = '%7C'.join(VALID_EVENTS)
        params['interval'] = interval if interval in VALID_INTERVALS else '1m'
        params['includeAdjustedClose'] = 'true' if adj_close else 'false'
        params['includePrePost'] = 'true' if prepost else 'false'
        params['useYfid'] = 'true' if True else 'false'  # ???
        params['formatted'] = 'false'
        url = self._build_url(ENDPOINTS['chart'])
        return await self._get_json(url, params=params)

    async def download_chart(self, symbol: str, start: int = 0, end: int = 0, interval: str = '1d', adj_close: bool = True):
        params = dict()
        if end == 0:
            end = int(time.time())
        if start >= end or start == 0:
            params['period1'] = end - 86400 * 5
            params['period2'] = end
        else:
            params['period1'] = start
            params['period2'] = end
        params['interval'] = interval if interval in ('1d', '1wk', '1mo') else '1d'
        params['includeAdjustedClose'] = 'true' if adj_close else 'false'
        params['events'] = 'history'
        url = self._build_url(ENDPOINTS['download']) + symbol
        return await self._get_text(url, params=params)

    async def get_options(self, symbol: str, expiration: int = 0):
        params = dict()
        params['symbol'] = symbol
        if expiration > 0:
            params['date'] = expiration
        params['formatted'] = 'false'
        url = self._build_url(ENDPOINTS['options'])
        return await self._get_json(url, params=params)

    async def search(self, text: str):
        params = dict()
        params['q'] = text
        url = self._build_url(ENDPOINTS['search'])
        return await self._get_json(url, params=params)

    async def get_recommendations(self, symbol: str | list):
        params = dict()
        params['symbol'] = ','.join(symbol) if isinstance(symbol, list) else symbol
        params['formatted'] = 'false'
        url = self._build_url(ENDPOINTS['recommendations'])
        return await self._get_json(url, params=params)


class YahooFinanceSyncFetcher(metaclass=SingletonMeta):

    def __init__(self):
        self._session = requests.Session()
        self._timeout = 30
        self._crumb = ''

    def _get(self, url: str, params: dict = None):
        resp = self._session.get(url, params=params, timeout=self._timeout, headers={'User-Agent': USER_AGENT})
        return resp

    def _post(self, url: str, data: dict = None):
        resp = self._session.post(url, data=data, timeout=self._timeout, headers={'User-Agent': USER_AGENT})
        return resp

    def _get_text(self, url: str, params: dict = None):
        try:
            resp = self._get(url, params=params)
            return resp.text
        except requests.RequestException as e:
            raise e

    def _get_json(self, url: str, params: dict = None, with_crumb: bool = False, first_attempt: bool = True):
        if with_crumb:
            if params is None:
                params = dict()
            params['crumb'] = self._get_crumb()
        try:
            resp = self._get(url, params=params)
            return resp.json()
        except requests.RequestException as e:
            if e.response.status_code == 401 and with_crumb and first_attempt:
                self._update_crumb()
                return self._get_json(url, params=params, with_crumb=with_crumb, first_attempt=False)
            else:
                raise e

    def _build_url(self, path: str) -> str:
        return random.choice(API_URLS) + path

    def _get_yahoo_cookies(self):
        resp = self._session.get("https://guce.yahoo.com/consent", timeout=self._timeout, allow_redirects=True, headers={'User-Agent': USER_AGENT})
        # todo: check non-EU countries steps
        # we can check resp.history for redirects
        if 'consent.yahoo.com' in resp.url:
            # should accept cookies
            content = resp.text
            soup = BeautifulSoup(content, 'html.parser')
            csrfTokenInput = soup.find('input', attrs={'name': 'csrfToken'})
            csrfToken = csrfTokenInput['value']
            sessionIdInput = soup.find('input', attrs={'name': 'sessionId'})
            sessionId = sessionIdInput['value']
            originalDoneUrl = 'https://finance.yahoo.com/'
            namespace = 'yahoo'
            data = [
                ('agree', 'agree'),
                ('agree', 'agree'),
                ('consentUUID', 'default'),
                ('sessionId', sessionId),
                ('csrfToken', csrfToken),
                ('originalDoneUrl', originalDoneUrl),
                ('namespace', namespace),
            ]
            self._session.post(f'https://consent.yahoo.com/v2/collectConsent?sessionId={sessionId}', data=data, allow_redirects=True, headers={'User-Agent': USER_AGENT})

    def _update_crumb(self, first_attempt: bool = True):
        url = self._build_url(ENDPOINTS['crumb'])
        try:
            resp = self._get_text(url)
            if resp == '':
                if first_attempt:
                    self._get_yahoo_cookies()
                    self._update_crumb(first_attempt=False)
                else:
                    raise RuntimeError('Failed to get crumb')
            else:
                self._crumb = resp
        except requests.RequestException as e:
            if e.response.status_code == 429:
                if first_attempt:
                    self._get_yahoo_cookies()
                    return self._update_crumb(first_attempt=False)
            raise e

    def _get_crumb(self) -> str:
        if self._crumb == '':
            self._update_crumb()
        return self._crumb

    def warm_up(self):
        self._get_yahoo_cookies()
        self._get_crumb()

    def get_spark(self, symbols: str | list, range_: str = '1d', interval: str = '5m'):
        params = dict()
        params['symbols'] = ','.join(symbols) if isinstance(symbols, list) else symbols
        params['range'] = range_ if range_ in VALID_RANGES else '1d'
        params['interval'] = interval if interval in VALID_INTERVALS else '5m'
        params['formatted'] = 'false'
        url = self._build_url(ENDPOINTS['spark'])
        return self._get_json(url, params=params)

    def get_quote_type(self, symbols: str | list):
        params = dict()
        params['symbol'] = ','.join(symbols) if isinstance(symbols, list) else symbols
        url = self._build_url(ENDPOINTS['quote_type'])
        return self._get_json(url, params=params)

    def get_quote(self, symbols: str | list):  # requires crumb
        params = dict()
        params['symbols'] = ','.join(symbols) if isinstance(symbols, list) else symbols
        params['formatted'] = 'false'
        url = self._build_url(ENDPOINTS['quote'])
        return self._get_json(url, params=params, with_crumb=True)

    def get_quote_summary(self, symbol: str, modules: list = None):  # requires crumb
        params = dict()
        params['symbol'] = symbol
        if modules is not None:
            params['modules'] = ','.join(modules)
        else:
            params['modules'] = ','.join(VALID_MODULES)
        params['formatted'] = 'false'
        url = self._build_url(ENDPOINTS['quote_summary'])
        return self._get_json(url, params=params, with_crumb=True)

    def get_non_stock_quote_summary(self, symbol: str):  # requires crumb
        params = dict()
        params['symbol'] = symbol
        params['modules'] = ','.join(NON_STOCK_MODULES)
        params['formatted'] = 'false'
        url = self._build_url(ENDPOINTS['quote_summary'])
        return self._get_json(url, params=params, with_crumb=True)

    def get_chart(self, symbol: str, start: int = 0, end: int = 0, interval: str = '5m', prepost: bool = False, adj_close: bool = True, events: list = None):
        params = dict()
        params['symbol'] = symbol
        if end == 0:
            end = int(time.time())
        if start >= end or start == 0:
            params['period1'] = end - 86400 * 7
            params['period2'] = end
        else:
            params['period1'] = start
            params['period2'] = end
        if events is not None:
            if len(events):
                params['events'] = '%7C'.join(events)  # | sepperated
        else:
            params['events'] = '%7C'.join(VALID_EVENTS)
        params['interval'] = interval if interval in VALID_INTERVALS else '1m'
        params['includeAdjustedClose'] = 'true' if adj_close else 'false'
        params['includePrePost'] = 'true' if prepost else 'false'
        params['useYfid'] = 'true' if True else 'false'  # ???
        params['formatted'] = 'false'
        url = self._build_url(ENDPOINTS['chart'])
        return self._get_json(url, params=params)

    def download_chart(self, symbol: str, start: int = 0, end: int = 0, interval: str = '1d', adj_close: bool = True):
        params = dict()
        if end == 0:
            end = int(time.time())
        if start >= end or start == 0:
            params['period1'] = end - 86400 * 30
            params['period2'] = end
        else:
            params['period1'] = start
            params['period2'] = end
        params['interval'] = interval if interval in ('1d', '1wk', '1mo') else '1d'
        params['includeAdjustedClose'] = 'true' if adj_close else 'false'
        params['events'] = 'history'
        url = self._build_url(ENDPOINTS['download']) + symbol
        return self._get_text(url, params=params)

    def get_options(self, symbol: str, expiration: int = 0):
        params = dict()
        params['symbol'] = symbol
        if expiration > 0:
            params['date'] = expiration
        params['formatted'] = 'false'
        url = self._build_url(ENDPOINTS['options'])
        return self._get_json(url, params=params)

    def search(self, text: str):
        params = dict()
        params['q'] = text
        url = self._build_url(ENDPOINTS['search'])
        return self._get_json(url, params=params)

    def get_recommendations(self, symbol: str | list):
        params = dict()
        params['symbol'] = ','.join(symbol) if isinstance(symbol, list) else symbol
        params['formatted'] = 'false'
        url = self._build_url(ENDPOINTS['recommendations'])
        return self._get_json(url, params=params)
