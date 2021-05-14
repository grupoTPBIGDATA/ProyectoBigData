from dataclasses import dataclass
from datetime import datetime


@dataclass
class Precio:
    id_criptomoneda: str
    precio: float
    fecha: datetime
