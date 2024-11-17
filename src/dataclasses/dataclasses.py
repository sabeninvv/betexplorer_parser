from typing import Optional, List, Dict
from dataclasses import dataclass
import base64
import pickle


@dataclass(frozen=False)
class Match:
    uri: Optional[str] = None
    bookmakers: Optional[List[str]] = None
    id: Optional[str] = None
    name: Optional[str] = None
    team_1_bid_range: Optional[Dict[str, float]] = None
    team_2_bid_range: Optional[Dict[str, float]] = None

    def __post_init__(self):
        if self.id is not None:
            dump: 'Match' = pickle.loads(base64.urlsafe_b64decode(self.id))
            self.__dict__ = dump.__dict__
        else:
            self.uri = self.uri[:-1] if self.uri[-1] == "/" else self.uri
            self.name = " => ".join(self.uri.split("/")[3:-2 if self.uri[-1] == "/" else -1])
        self.id = base64.urlsafe_b64encode(pickle.dumps(self)).decode("UTF-8")
