import json
from os import getenv
from typing import List, Set
from time import sleep

import requests
from bs4 import BeautifulSoup
from redis import StrictRedis

from src.config import *
from src.logger import logger
from src.tools import decoding_bytes2str, retry
from src.dataclasses import Match
from src.tools import text_preprocessing, parse_bid_ranges


class SimplenoteParser:
    def __init__(self, **kwargs):
        self.loagger = kwargs['logger']
        self._name = "SimplenoteParser"
        self.__last_ts = 0
        self.__inmonitoring_matches = set()
        self.__endmonitoring_matches = set()
        self._connection: StrictRedis = StrictRedis(password=getenv('REDIS_PASSWORD'))

    def convert_to_match(self, response: str) -> Match:
        response = text_preprocessing(response)
        uri, bookmakers, *diaps = response.split("|")
        if bookmakers:
            bookmakers = bookmakers.split(",")
        else:
            bookmakers = ['10bet', '1xbet', 'bet-at-home', 'betvictor', 'betway',
                          'bwin', 'comeon', 'pinnacle', 'unibet', 'william hill',
                          'youwin', 'betfair exchange']
        bookmakers = [bookmaker.lower() for bookmaker in bookmakers]

        return Match(uri=uri, bookmakers=bookmakers, **parse_bid_ranges(diaps))

    def get_html_from_simplenote(self, uri: str) -> bytes:
        return requests.get(url=uri).content

    def parse_html(self, html: bytes) -> List[str]:
        soup = BeautifulSoup(html, 'lxml')
        rows = soup.find('div', {'class': "note note-detail-markdown"}).find_all('h1')
        return [row.text for row in rows]

    def get_requests(self) -> List[str]:
        while True:
            rows_from_evernote = self.parse_html(html=self.get_html_from_simplenote(uri=getenv('URI_SIMPLENOTE')))
            if not rows_from_evernote:
                sleep(60.)
                continue
            return rows_from_evernote

    @property
    def inmonitoring_matches(self) -> Set[str]:
        return decoding_bytes2str(self._connection.smembers(INMONITORING_MATCHES)) | self.endmonitoring_matches | self.inflight_matches

    @property
    def endmonitoring_matches(self) -> Set[str]:
        self.__endmonitoring_matches: List[bytes] = self._connection.smembers(ENDMONITORING_MATCHES)
        return decoding_bytes2str(self.__endmonitoring_matches)

    @property
    def inflight_matches(self) -> Set[str]:
        self.__inflight_matches: List[bytes] = self._connection.smembers(INFLIGHTING_MATCHES)
        return decoding_bytes2str(self.__inflight_matches)

    def send_matches(self, matches: List[Match]):
        if not matches:
            return

        self._connection.rpush(
            'queue:request',
            *[json.dumps(match.__dict__) for match in matches]
        )
        _ = [logger.info(f"[{self._name}] put2queue {match.uri}, {match.bookmakers}")
             for match in matches]

    def get_hashes_of_completed_matches(self) -> Set[bytes]:
        return self._connection.smembers(COMPLETED_MATCHES)

    @retry(logger=logger, num_tries=float("inf"), idle_time_sec=3.)
    def filling_request(self):
        """
        Получить и распарсить данные от клиента
        Получить данные из inmonitoring_matches
        Выявить данные которых нету в inmonitoring_matches
        Записать данные в inmonitoring
        :return:
        """
        while True:
            rows_from_evernote = self.get_requests()
            wanted_matches: List[Match] = [self.convert_to_match(request) for request in set(rows_from_evernote)]
            hashes_of_completed_matches: Set[bytes] = self.get_hashes_of_completed_matches()
            wanted_matches = [match for match in wanted_matches if match.id.encode() not in hashes_of_completed_matches]
            self.send_matches(matches=wanted_matches)
            sleep(60.)

