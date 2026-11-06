from datetime import datetime
from pydantic import BaseModel


class FileJobResponse(BaseModel):
    id: int
    filename: str
    status: str
    total_rows: int
    processed_rows: int
    error_message: str | None = None
    created_at: datetime

    class Config:
        from_attributes = True