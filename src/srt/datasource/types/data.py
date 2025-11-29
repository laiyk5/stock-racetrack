from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class TimePointData(BaseModel):
    time: datetime


class Entity(BaseModel):
    id: Optional[int] = Field(default=None)


class TimeRangeData(BaseModel):
    start_time: datetime
    end_time: datetime
