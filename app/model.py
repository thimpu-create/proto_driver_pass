from pydantic import BaseModel

class RideRequest(BaseModel):
    passenger_id: str
    lat: float
    lon: float
