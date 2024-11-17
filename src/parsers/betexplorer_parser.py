import json
import asyncio


from os import getenv
from typing import Optional, List, Set, Any, Dict, Union
from time import sleep
from collections import defaultdict


from aiohttp import ClientSession, ClientTimeout, TCPConnector
from bs4 import BeautifulSoup
from redis import StrictRedis

from src.config import *
from src.logger import logger
from src.service import TelegramClient
from src.dataclasses import Match


class BetExplorerParser:
    def __init__(self, **kwargs):
        self._name = "[BetExplorerParser]"
        self._futures = set()
        self.num_semaphore_limits = 1_000
        self._redis_conn = StrictRedis(password=getenv('REDIS_PASSWORD', None))
        self._session: ClientSession = kwargs['aiohttp_session']
        self.telegram_cli: TelegramClient = kwargs['telegram_cli']

    async def get_html_match_info(self, uri: str) -> Dict[str, Any]:
        tail = -2 if uri[-1] == "/" else -1
        match_id = uri.split("/")[tail]

        cookies = {'my_timezone': '%2B1'}

        headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'ru-RU,ru;q=0.8',
            'if-modified-since': 'Fri, 01 Nov 2024 10:07:16 GMT',
            'priority': 'u=1, i',
            'referer': uri,
            'sec-ch-ua': '"Chromium";v="130", "Brave";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sec-gpc': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }
        postfix = "/0/ha/1/en/"
        async with self._session.get(url=f'https://www.betexplorer.com/match-odds-old/{match_id}{postfix}',
                                     cookies=cookies, headers=headers) as resp:
            resp.raise_for_status()
            return await resp.json(content_type="text/plain")

    async def get_bookmakers_from_site(self, uri) -> Optional[Dict[str, Dict[int, float]]]:
        try:
            html_match_info = await self.get_html_match_info(uri=uri)
            soup = BeautifulSoup(html_match_info["odds"], 'lxml')

            mapping = {}
            for block in soup.find_all('tr'):
                bookmaker_id = block.get('data-bid')
                if bookmaker_id is not None:
                    mapping[bookmaker_id] = block.find('td', {'class': 'h-text-left over-s-only'}).text.lower()

            stocks = defaultdict(dict)
            blocks = soup.find_all('td')
            if not blocks:
                return None

            for block in blocks:
                odd = block.get('data-odd')
                if odd is not None:
                    bookmaker_id = block.get('data-bid')
                    if bookmaker_id is None:
                        continue
                    bookmaker_name = mapping[bookmaker_id]
                    is_first = any("first" in tag for tag in block.get('class'))
                    if is_first:
                        stocks[bookmaker_name][1] = float(odd)
                    else:
                        stocks[bookmaker_name][2] = float(odd)
            mapping.clear()
            return stocks
        except Exception as ex:
            logger.error(f"{uri}", exc_info=True)
            return None

    def handler(self):
        start_val_for_hmap = "0"
        while True:
            match: Optional[bytes] = self._redis_conn.lpop('queue:request')
            if match is None:
                sleep(10.)
                continue
            match: Match = Match(**json.loads(match))
            name_hmap_of_match = f"inprocessing:{match.id}"

            new_bookmakers = match.bookmakers
            bookmakers_from_hash = [bookmaker_.decode() for bookmaker_ in self._redis_conn.hkeys(name_hmap_of_match)]
            if bookmakers_from_hash:
                new_bookmakers = set(match.bookmakers) - set(bookmakers_from_hash)
            if new_bookmakers:
                with self._redis_conn.pipeline() as pipe:
                    _ = [pipe.hset(name_hmap_of_match, bookmaker, start_val_for_hmap)
                         for bookmaker in new_bookmakers]
                    pipe.sadd(INWAITING_MATCHES, match.id)
                    pipe.execute()
                logger.info(f"{self._name} [handler] Add books: {new_bookmakers} for {match.uri}")

    def cold_start(self):
        pattern ="inprocessing:"
        with self._redis_conn.pipeline() as pipe:
            pipe.keys(f"{pattern}*")
            pipe.smembers(INWAITING_MATCHES)
            matches_inprocessing, matches_inwaiting = pipe.execute()
            if matches_inprocessing:
                matches_inprocessing: List[bytes] = [match_hash.decode().replace(pattern, "").encode()
                                                     for match_hash in matches_inprocessing]
            matches2add: Set[bytes] = set(matches_inwaiting) - set(matches_inprocessing)
            if matches2add:
                pipe.sadd(INWAITING_MATCHES, *list(matches2add))
                pipe.execute()
                logger.info(f"{self._name} [cold_start] Add {len(matches2add)} to {INWAITING_MATCHES}")

    def cancel_monitoring_match(self, match: Match):
        with self._redis_conn.pipeline() as pipe:
            pipe.srem(INWAITING_MATCHES, match.id)
            pipe.sadd(COMPLETED_MATCHES, match.id)
            pipe.execute()
        logger.info(f"{self._name} [run_event] Empty or full hmap for {match.uri}. "
                    f"Del from {INWAITING_MATCHES}. Add to {COMPLETED_MATCHES}")

    def get_bookmakers_meta_from_match(self, match: Match):
        name_hmap_of_match = f"inprocessing:{match.id}"
        bookmakers_meta: Dict[bytes, bytes] = self._redis_conn.hgetall(name_hmap_of_match)
        return {bookmaker.decode(): status.decode() for bookmaker, status in bookmakers_meta.items()
                if status not in (1, "1", b"1")}

    def mark_bookmaker_found(self, match: Match, bookmaker):
        name_hmap_of_match = f"inprocessing:{match.id}"
        with self._redis_conn.pipeline() as pipe:
            pipe.hset(name_hmap_of_match, bookmaker, "1")
            pipe.expire(name_hmap_of_match, 14 * 24 * 60 * 60)
            pipe.execute()

    async def send_notification_to_telegram(self, bookmaker: Union[str, bytes], match: Match, meta):
        self.mark_bookmaker_found(match=match, bookmaker=bookmaker)
        msg = f"{match.name} | found: {bookmaker} | koefs: {meta.get(bookmaker)}"
        await self.telegram_cli.send_msg2all_chats(msg=msg)
        logger.info(f"{self._name} [run_event] {msg}")

    async def wrap_math_monitoring(self, match: Match):
        try:
            return await self.math_monitoring(match=match)
        except Exception as ex:
            logger.error(match, exc_info=True)
            return match

    async def math_monitoring(self, match: Match) -> Match:
        while True:
            bookmakers_meta: Dict[bytes, bytes] = self.get_bookmakers_meta_from_match(match=match)
            if not bookmakers_meta:
                return match

            meta: dict = await self.get_bookmakers_from_site(uri=match.uri)
            if meta is None:
                return match

            tasks = (self.send_notification_to_telegram(match=match, bookmaker=bookmaker, meta=meta)
                     for bookmaker in bookmakers_meta if self.condition(match=match, meta=meta))
            await asyncio.gather(*tasks, return_exceptions=False)

            await asyncio.sleep(1.)

    def schedule_future_result(self, future: asyncio.Future):
        self._futures.remove(future)
        match: Match = future.result()
        self.cancel_monitoring_match(match=match)

    def condition(self, match: Match, meta: Dict[str, Dict[int, float]]):
        bookmaker = match.bookmakers[0]
        cond_1, cond_2 = False, False
        bookmaker_meta = meta[bookmaker]
        if not bookmaker_meta:
            return False

        if match.team_1_bid_range is not None:
            cond_1 = match.team_1_bid_range['min'] < bookmaker_meta[1] < match.team_1_bid_range['max']
        if match.team_2_bid_range is not None:
            cond_2 = match.team_2_bid_range['min'] < bookmaker_meta[2] < match.team_2_bid_range['max']
        return any([cond_1, cond_2])

    def get_match_hash(self) -> Optional[bytes]:
        return self._redis_conn.spop(INWAITING_MATCHES)

    async def ride_round_robin(self):
        while True:
            match_hash: bytes = self.get_match_hash()
            if match_hash:
                match = Match(id=match_hash)
                if "basketball" in match.uri:
                    self.create_future(match=match)
            await asyncio.sleep(.01)

    def create_future(self, match: Match):
        future: asyncio.Future = asyncio.ensure_future(coro_or_future=self.wrap_math_monitoring(match=match))
        self._futures.add(future)
        future.add_done_callback(self.schedule_future_result)
