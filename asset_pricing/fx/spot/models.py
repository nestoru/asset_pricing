# asset_pricing/fx/spot/models.py

from pydantic import BaseModel
from datetime import datetime

class FXSpotRate(BaseModel):
    symbol: str
    date: datetime
    rate: float

