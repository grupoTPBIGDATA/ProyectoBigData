from dataclasses import dataclass
from datetime import datetime


@dataclass
class Precio:
    id_criptomoneda: str
    precio: float
    fecha: datetime

@dataclass
class MarketCap:
    id_criptomoneda: str
    market_cap: float
    fecha: datetime

@dataclass
class Volume:
    id_criptomoneda: str
    volume: float
    fecha: datetime


@dataclass
class Tweet:
    id: str
    tweet: str
    hashtag: any
    cashtag: any
    fecha: datetime
    user: str
    name: str
    link: str
    search: str
    type = "tweet"
