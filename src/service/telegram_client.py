import asyncio
from typing import Set, List
from os import getenv
from aiohttp import ClientSession


class TelegramClient:
    def __init__(self, session: ClientSession, **kwargs):
        token = kwargs.get('token', getenv('TELEGRAM_TOKEN'))
        self.__uri = f"https://api.telegram.org/bot{token}"
        self._session = session
        self.default_chat_ids = getenv('TELEGRAM_PHONES4PUSH', None)
        if self.default_chat_ids is not None:
            self.default_chat_ids: List[str] = self.default_chat_ids.split(",")

    async def get_chat_ids(self) -> Set[str]:
        try:
            async with self._session.get(url=f"{self.__uri}/getUpdates") as response:
                response.raise_for_status()
                response = await response.json()
                seq_msgs = response['result']
                if not seq_msgs:
                    return set(self.default_chat_ids)
                chat_ids = [msg.get('message', {}).get('chat', {}).get('id', None) for msg in seq_msgs]
                if self.default_chat_ids is not None:
                    chat_ids += self.default_chat_ids
                return set(_id for _id in chat_ids if _id is not None)
        except Exception as ex:
            return set(self.default_chat_ids)

    async def send_msg2chat(self, chat_id: str, msg: str):
        return await self._session.post(url=f"{self.__uri}/sendMessage",
                                        params={'chat_id': chat_id, 'text': msg})

    async def send_msg2all_chats(self, msg: str):
        chat_ids = await self.get_chat_ids()
        tasks  = (self.send_msg2chat(chat_id=chat_id, msg=msg) for chat_id in chat_ids)
        await asyncio.gather(*tasks, return_exceptions=False)
